package tech.ydb.samples.exporter;

import com.google.gson.Gson;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
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
    private final Gson gson;

    private final AtomicBoolean shouldRun = new AtomicBoolean(false);
    private final AtomicReference<ExecutorService> es = new AtomicReference<>();
    private final AtomicLong numberOfJobsScheduled = new AtomicLong(0);
    private final ArrayList<Throwable> jobFailures = new ArrayList<>();

    private final ArrayBlockingQueue<ArrayList<String[]>> outputQueue;
    private final AtomicReference<Thread> outputThread  = new AtomicReference<>();

    public Tool(YdbConnector yc, JobDef job) {
        this.yc = yc;
        this.job = job;
        this.retryCtx = SessionRetryContext.create(yc.getQueryClient()).build();
        this.gson = new Gson();
        this.outputQueue = new ArrayBlockingQueue<>(1000);
    }

    @Override
    public void close() {
        shutdownExecutors();
    }

    @Override
    public void run() {
        if (shouldRun.get() || es.get() != null) {
            throw new IllegalStateException("Already running");
        }
        LOG.info("Initializing parallel exporter...");
        try {
            initExecutors();
        } catch(Exception ex) {
            throw new RuntimeException("Initialization failed", ex);
        }
        if (job.isUseMainQueryPaging()) {
            mainPagedRead();
        } else {
            mainSingleRead();
        }
        waitJobs();
        shutdownExecutors();
        LOG.info("Parallel exporter job completed.");
    }

    private synchronized void initExecutors() throws Exception {
        if (es.get() != null || outputThread.get() != null) {
            throw new IllegalStateException("Already initialized");
        }
        shouldRun.set(true);
        es.set(
                Executors.newFixedThreadPool(job.getWorkerCount(),
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
        Thread outputThreadTemp = new Thread(new OutputWorker());
        outputThreadTemp.setName("ydb-exporter-output");
        outputThreadTemp.setDaemon(true);
        outputThreadTemp.start();
        outputThread.set(outputThreadTemp);
    }

    private synchronized void shutdownExecutors() {
        shouldRun.set(false);
        ExecutorService temp = es.getAndSet(null);
        if (temp!=null) {
            temp.shutdownNow();
        }
        outputThread.set(null);
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
        LOG.info("Background scanner tasks completed.");
        if (outputThread.get() != null) {
            try {
                // Sign to stop
                outputQueue.put(new ArrayList<>());
                // Wait for actual stop
                LOG.info("Waiting for output task completion...");
                outputThread.get().join();
            } catch(InterruptedException ix) {
                throw new RuntimeException("Interrupted on output queue flush");
            }
            LOG.info("Output task completed.");
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
        
        LOG.info("Main query completed.");
    }

    private void mainSingleRead() {
        LOG.info("Performing single-action read for the main query.");
        try (QuerySession qs = yc.createQuerySession()) {
            qs.createQuery(job.getMainQuery(), TxMode.SNAPSHOT_RO)
                    .execute(part -> submitMainPart(part.getResultSetReader()))
                    .join()
                    .getStatus()
                    .expectSuccess("Main query failed");
        }
        LOG.info("Main query completed.");
    }
    
    private void submitMainPart(ResultSetReader rsr) {
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

    private void processMainPart(ResultSetReader input) {
        Value<?>[] rows;
        if (job.getDetailsInput().isEmpty()) {
            rows = collectMainKeys1(input);
        } else {
            rows = collectMainKeys2(input);
        }
        grabDetails(ListValue.of(rows));
    }

    private Value<?>[] collectMainKeys1(ResultSetReader input) {
        Value<?>[] rows = new StructValue[input.getRowCount()];
        int rownum = 0;
        while (input.next()) {
            HashMap<String, Value<?>> m = new HashMap<>();
            for (int column = 0; column < input.getColumnCount(); ++column) {
                m.put(input.getColumnName(column), input.getColumn(column).getValue());
            }
            rows[rownum++] = StructValue.of(m);
        }
        return rows;
    }

    private Value<?>[] collectMainKeys2(ResultSetReader input) {
        Value<?>[] rows = new StructValue[input.getRowCount()];
        int[] indexes = new int[job.getDetailsInput().size()];
        for (int ix = 0; ix < indexes.length; ++ix) {
            String column = job.getDetailsInput().get(ix);
            indexes[ix] = input.getColumnIndex(column);
            if (indexes[ix] < 0) {
                LOG.warn("Missing column {} in main query output", column);
            }
        }
        int rownum = 0;
        while (input.next()) {
            HashMap<String, Value<?>> m = new HashMap<>();
            for (int ix = 0; ix < indexes.length; ++ix) {
                if (indexes[ix] > 0) {
                    int index = indexes[ix];
                    m.put(input.getColumnName(index), input.getColumn(index).getValue());
                }
            }
            rows[rownum++] = StructValue.of(m);
        }
        return rows;
    }

    private void grabDetails(ListValue input) {
        String query = job.getDetailsQuery();
        Params params = Params.of("$input", input);
        QueryReader result = retryCtx.supplyResult(
                session -> QueryReader.readFrom(
                        session.createQuery(query, TxMode.SERIALIZABLE_RW, params))
        ).join().getValue();
        pushDetailsToOutput(result);
    }

    private void pushDetailsToOutput(QueryReader output) {
        for (int i = 0; i < output.getResultSetCount(); ++i) {
            ResultSetReader rsr = output.getResultSet(i);
            pushDetailsToOutput(rsr);
        }
    }

    private void pushDetailsToOutput(ResultSetReader output) {
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
    
    private CharSequence formatJson(ArrayList<String[]> block) {
        if (block.size() <= 1) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();
        String[] columns = block.get(0);
        final HashMap<String,String> m = new HashMap<>();
        for (int i=1; i<block.size(); ++i) {
            m.clear();
            String[] values = block.get(i);
            for (int j=0; j<columns.length && j<values.length; ++j) {
                m.put(columns[j], values[j]);
            }
            gson.toJson(m, sb);
            sb.append("\n");
        }
        return sb;
    }
    
    private CharSequence formatCsv(boolean first, ArrayList<String[]> block) {
        if (block.size() <= 1) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();
        try {
            CSVPrinter cp;
            if (JobDef.Format.TSV.equals(job.getOutputFormat())) {
                cp = new CSVPrinter(sb, CSVFormat.POSTGRESQL_TEXT);
            } else {
                cp = new CSVPrinter(sb, CSVFormat.POSTGRESQL_CSV);
            }
            if (first) {
                cp.printRecord(Arrays.asList(block.get(0)));
            }
            for (int i=1; i<block.size(); ++i) {
                cp.printRecord(Arrays.asList(block.get(i)));
            }
        } catch(IOException iox) {
            throw new RuntimeException("Failed to format CSV block", iox);
        }
        return sb;
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

    private class OutputWorker implements Runnable {
        
        final Writer writer;
        final boolean own;
        
        OutputWorker() throws IOException {
            String fname = job.getOutputFile();
            if (fname.isEmpty() || fname.equalsIgnoreCase("-")) {
                this.writer = new PrintWriter(System.out);
                this.own = false;
            } else {
                this.writer = new OutputStreamWriter(
                                new FileOutputStream(fname), StandardCharsets.UTF_8);
                this.own = true;
            }
        }
        
        @Override
        public void run() {
            try {
                doRun();
            } catch(Exception ex) {
                LOG.error("Failed to write to output file {}", job.getOutputFile(), ex);
            } finally {
                if (own) {
                    try {
                        writer.close();
                    } catch(Exception err) {
                        LOG.error("Failed to close output file {}", job.getOutputFile(), err);
                    }
                }
            }
        }
        
        void doRun() throws IOException {
            boolean first = true;
            while (true) {
                final ArrayList<String[]> block;
                try {
                    block = outputQueue.take();
                } catch(InterruptedException ix) {
                    continue;
                }
                if (block==null || block.isEmpty()) {
                    break;
                }
                CharSequence v;
                if (JobDef.Format.JSON.equals(job.getOutputFormat())) {
                    v = formatCsv(first, block);
                } else {
                    v = formatJson(block);
                }
                if (v!=null && v.length() > 0) {
                    writer.append(v);
                }
                first = false;
            }
            writer.flush();
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
                processMainPart(rsr);
            } catch(Exception ex) {
                processException(ex);
            }
            numberOfJobsScheduled.decrementAndGet();
        }
    }
}
