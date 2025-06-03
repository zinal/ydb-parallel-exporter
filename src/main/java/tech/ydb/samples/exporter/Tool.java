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

    private final ArrayBlockingQueue<ArrayList<Object[]>> outputQueue;
    private final AtomicReference<Thread> outputThread  = new AtomicReference<>();
    private final Stats stats = new Stats();

    public Tool(YdbConnector yc, JobDef job) {
        this.yc = yc;
        this.job = job;
        this.retryCtx = SessionRetryContext.create(yc.getQueryClient()).build();
        this.gson = new Gson();
        this.outputQueue = new ArrayBlockingQueue<>(job.getQueueSize());
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
        if (job.hasPageQuery()) {
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
        stats.start();
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
        LOG.error("*** Total {} sub-job failures detected.", errors.size());
        for (Throwable err : errors) {
            LOG.error("Sub-task error", err);
        }
        return true;
    }

    private void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch(InterruptedException ix) {
            Thread.currentThread().interrupt();
        }
    }
    
    private TxMode getIsolation() {
        if (job.getIsolation()==null) {
            return TxMode.SERIALIZABLE_RW;
        } else {
            return job.getIsolation();
        }
    }
    
    private void mainPagedRead() {
        LOG.info("Performing paged reads for the main query.");

        QueryReader result = retryCtx.supplyResult(
                session -> QueryReader.readFrom(
                        session.createQuery(job.getMainQuery(), getIsolation()))
        ).join().getValue();

        while (shouldRun.get()) {
            for (int pos = 0; pos < result.getResultSetCount(); ++pos) {
                submitMainPart(result.getResultSet(pos));
            }
            StructValue input = collectPagedKey(result);
            if (input==null) {
                // Empty input means end of data.
                break;
            }
            Params params = Params.of("$input", input);
            result = retryCtx.supplyResult(
                    session -> QueryReader.readFrom(
                            session.createQuery(job.getPageQuery(), getIsolation(), params))
            ).join().getValue();
        }

        if (! shouldRun.get()) {
            LOG.warn("Main query canceled.");
        } else {
            LOG.info("Main query completed.");
        }
    }

    private StructValue collectPagedKey(QueryReader result) {
        StructValue sv = null;
        for (int pos = 0; pos < result.getResultSetCount(); ++pos) {
            ResultSetReader rsr = result.getResultSet(pos);
            if (rsr.getRowCount()==0) {
                continue;
            }
            rsr.setRowIndex(rsr.getRowCount() - 1);
            HashMap<String,Value<?>> m = new HashMap<>();
            for (String column : job.getPageInput()) {
                m.put(column, rsr.getColumn(column).getValue());
            }
            sv = StructValue.of(m);
        }
        return sv;
    }

    private void mainSingleRead() {
        LOG.info("Performing single-action read for the main query.");
        try (QuerySession qs = yc.createQuerySession()) {
            qs.createQuery(job.getMainQuery(), getIsolation())
                    .execute(part -> submitMainPart(part.getResultSetReader()))
                    .join()
                    .getStatus()
                    .expectSuccess("Main query failed");
        }
        LOG.info("Main query completed.");
    }

    private void submitMainPart(ResultSetReader rsr) {
        if (shouldRun.get()) {
            stats.updateInput(rsr);
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
        if (input.getRowCount() < 1) {
            return;
        }
        input.setRowIndex(0);
        Value<?>[] rows;
        if (job.getDetailsInput().isEmpty()) {
            rows = collectDetailsKeys1(input);
        } else {
            rows = collectDetailsKeys2(input);
        }
        stats.updateInside(rows);
        grabDetails(ListValue.of(rows));
    }

    private Value<?>[] collectDetailsKeys1(ResultSetReader input) {
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

    private Value<?>[] collectDetailsKeys2(ResultSetReader input) {
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
                        session.createQuery(query, getIsolation(), params))
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
        ArrayList<Object[]> batch = new ArrayList<>(1 + output.getRowCount());
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
    
    private CharSequence formatJson(ArrayList<Object[]> block) {
        if (block.size() <= 1) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();
        Object[] columns = block.get(0);
        final HashMap<String,Object> m = new HashMap<>();
        for (int i=1; i<block.size(); ++i) {
            m.clear();
            Object[] values = block.get(i);
            for (int j=0; j<columns.length && j<values.length; ++j) {
                m.put(columns[j].toString(), values[j]);
            }
            gson.toJson(m, sb);
            sb.append("\n");
        }
        return sb;
    }
    
    private CharSequence formatCsv(boolean first, ArrayList<Object[]> block) {
        if (block.size() <= 1) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();
        try {
            CSVPrinter cp;
            if (JobDef.Format.TSV.equals(job.getOutputFormat())) {
                cp = new CSVPrinter(sb, CSVFormat.TDF);
            } else {
                cp = new CSVPrinter(sb, CSVFormat.RFC4180);
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
            System.out.println("USAGE: Tool connection.xml job.xml");
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
                final ArrayList<Object[]> block;
                try {
                    block = outputQueue.take();
                } catch(InterruptedException ix) {
                    continue;
                }
                if (block==null || block.isEmpty()) {
                    break;
                }
                stats.updateOutput(block);
                CharSequence v;
                if (JobDef.Format.JSON.equals(job.getOutputFormat())) {
                    v = formatJson(block);
                } else {
                    v = formatCsv(first, block);
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
    
    private static class Stats {
        long rowsInput = 0L;
        long rowsInside = 0L;
        long rowsOutput = 0L;
        long rowsInputPrev = 0L;
        long rowsInsidePrev = 0L;
        long rowsOutputPrev = 0L;
        long tvLast = 0L;

        synchronized void start() {
            tvLast = System.currentTimeMillis();
            rowsInput = 0L;
            rowsOutput = 0L;
            rowsInputPrev = 0L;
            rowsOutputPrev = 0L;
        }

        synchronized void updateInput(ResultSetReader rsr) {
            rowsInput += rsr.getRowCount();
            reportProgressIf();
        }

        synchronized void updateInside(Value<?>[] block) {
            rowsInside += block.length;
            reportProgressIf();
        }

        synchronized void updateOutput(ArrayList<Object[]> block) {
            rowsOutput += block.size();
            reportProgressIf();
        }

        void reportProgressIf() {
            long tv = System.currentTimeMillis();
            if (tv - tvLast >= 10000L) {
                reportProgress(tv);
                tvLast = tv;
                rowsInputPrev = rowsInput;
                rowsInsidePrev = rowsInside;
                rowsOutputPrev = rowsOutput;
            }
        }

        private void reportProgress(long tv) {
            long input = rowsInput - rowsInputPrev;
            long inside = rowsInside - rowsInsidePrev;
            long output = rowsOutput - rowsOutputPrev;
            long millis = tv - tvLast;
            double rateInput = ((double)input) * 1000.0 / ((double)millis);
            double rateInside = ((double)inside) * 1000.0 / ((double)millis);
            double rateOutput = ((double)output) * 1000.0 / ((double)millis);
            LOG.info("PROGRESS (input/inside/output) {}/{}/{} rows, {}/{}/{} rows per second.",
                    input, inside, output, String.format("%.2f", rateInput),
                    String.format("%.2f", rateInside), String.format("%.2f", rateOutput));
        }
    }
}
