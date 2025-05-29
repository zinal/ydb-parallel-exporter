package tech.ydb.samples.exporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.QuerySession;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author zinal
 */
public class Tool implements Runnable, AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Tool.class);

    private final YdbConnector yc;
    private final JobDef job;
    private final SessionRetryContext retryCtx;

    private final AtomicBoolean shouldRun = new AtomicBoolean(false);
    private final AtomicReference<ExecutorService> es = new AtomicReference<>();
    private final AtomicLong numberOfJobsScheduled = new AtomicLong(0);
    private final ArrayList<Throwable> jobFailures = new ArrayList<>();
    private final ArrayBlockingQueue<ArrayList<String[]>> outputQueue;

    public Tool(YdbConnector yc, JobDef job) {
        this.yc = yc;
        this.job = job;
        this.retryCtx = SessionRetryContext.create(yc.getQueryClient()).build();
        this.outputQueue = new ArrayBlockingQueue<>(1000);
    }

    @Override
    public void close() {
        shouldRun.set(false);
        ExecutorService temp = es.getAndSet(null);
        if (temp!=null) {
            temp.shutdownNow();
        }
    }

    @Override
    public void run() {
        if (shouldRun.get() || es.get() != null) {
            throw new IllegalStateException("Already running");
        }
        shouldRun.set(true);
        LOG.info("Initializing parallel exporter...");
        initExecutors();
        if (job.isUseMainQueryPaging()) {
            mainPagedRead();
        } else {
            mainSingleRead();
        }
        waitJobs();
        LOG.info("Parallel exporter job completed.");
    }

    private void initExecutors() {
        ExecutorService old = es.getAndSet(
                Executors.newFixedThreadPool(1 + job.getWorkerCount(),
                        new ThreadFactory() {
            final AtomicInteger threadCounter = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("ydb-exporter-worker-" 
                        + String.valueOf(threadCounter.incrementAndGet()));
                t.start();
                return t;
            }
                }));
        if (old!=null) {
            old.shutdownNow();
            throw new IllegalStateException("Concurrent initialization");
        }
    }

    private void waitJobs() {
        while (numberOfJobsScheduled.get() > 0L) {
            boolean hasFailures;
            synchronized(jobFailures) {
                hasFailures = (!jobFailures.isEmpty());
            }
            if (hasFailures) {
                reportFailures();
                throw new RuntimeException("At least one of sub-jobs failed");
            }
            sleepMillis(50L);
        }
    }
    
    private boolean reportFailures() {
        final ArrayList<Throwable> errors;
        synchronized(jobFailures) {
            errors = new ArrayList<>(jobFailures);
            jobFailures.clear();
        }
        if (errors.isEmpty()) {
            return false;
        }
        LOG.info("*** Total {} sub-job failures detected.", errors.size());
        // TODO: group relevant messages and report
        return true;
    }

    private void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch(InterruptedException ix) {
            Thread.currentThread().interrupt();
        }
    }
    
    private void mainPagedRead() {
        LOG.info("Performing paged reads for the main query.");
        
    }

    private void mainSingleRead() {
        LOG.info("Performing single-action read for the main query.");
        try (QuerySession qs = yc.createQuerySession()) {
            qs.createQuery(job.getMainQuery(), TxMode.SNAPSHOT_RO)
                    .execute(part -> submitPart(part.getResultSetReader()))
                    .join()
                    .getStatus()
                    .expectSuccess("Main query failed");
        }
        LOG.info("Main query completed.");
    }
    
    private void submitPart(ResultSetReader rsr) {
        if (shouldRun.get()) {
            es.get().submit(new PartWorker(rsr));
        }
    }

    private void processException(Throwable ex) {
        shouldRun.set(false);
        synchronized(jobFailures) {
            jobFailures.add(ex);
        }
    }

    private void processPart(ResultSetReader input) {
        Value<?>[] rows = new StructValue[input.getRowCount()];
        int rownum = 0;
        while (input.next()) {
            HashMap<String, Value<?>> m = new HashMap<>();
            for (int column = 0; column < input.getColumnCount(); ++column) {
                m.put(input.getColumnName(column), input.getColumn(column).getValue());
            }
            rows[rownum++] = StructValue.of(m);
        }
        processPart(ListValue.of(rows));
    }

    private void processPart(ListValue input) {
        String query = job.getSubQuery();
        Params params = Params.of("$input", input);
        QueryReader result = retryCtx.supplyResult(
                session -> QueryReader.readFrom(
                        session.createQuery(query, TxMode.SERIALIZABLE_RW, params))
        ).join().getValue();
        pushRecordsToOutput(result);
    }

    private void pushRecordsToOutput(QueryReader output) {
        for (int i = 0; i < output.getResultSetCount(); ++i) {
            ResultSetReader rsr = output.getResultSet(i);
            pushRecordsToOutput(rsr);
        }
    }

    private void pushRecordsToOutput(ResultSetReader output) {
        if (output.getRowCount() < 1) {
            return;
        }
        String[] columnNames = new String[output.getColumnCount()];
        for (int column = 0; column < output.getColumnCount(); ++column) {
            columnNames[column] = output.getColumnName(column);
        }
        ArrayList<String[]> batch = new ArrayList<>(1 + output.getRowCount());
        batch.add(columnNames);
        while (output.next()) {
            batch.add(ValueConvertor.convertRecord(output));
        }
        while (shouldRun.get()) {
            try {
                if ( outputQueue.offer(batch, 100L, TimeUnit.MILLISECONDS) ) {
                    return;
                }
            } catch(InterruptedException ix) {}
        }
        LOG.info("Dropping the batch of {} records due to shutdown.", output.getRowCount());
    }
    
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("USAGE: App connection.xml job.xml");
            System.exit(1);
        }
        try {
            YdbConnector.Config ycc = YdbConnector.Config.fromFile(args[0]);
            JobDef job = JobDef.fromXml(args[1]);
            try (YdbConnector yc = new YdbConnector(ycc)) {
                try (Tool app = new Tool(yc, job)) {
                    app.run();
                }
            }
        } catch(Exception ex) {
            LOG.error("FATAL", ex);
        }
    }

    private class PartWorker implements Runnable {
        private final ResultSetReader rsr;

        public PartWorker(ResultSetReader rsr) {
            this.rsr = rsr;
            numberOfJobsScheduled.incrementAndGet();
        }

        @Override
        public void run() {
            try {
                processPart(rsr);
            } catch(Exception ex) {
                processException(ex);
            }
            numberOfJobsScheduled.decrementAndGet();
        }
    }
}
