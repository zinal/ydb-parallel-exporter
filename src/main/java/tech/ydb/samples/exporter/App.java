package tech.ydb.samples.exporter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.query.QuerySession;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.result.QueryResultPart;

/**
 *
 * @author zinal
 */
public class App implements Runnable, AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(App.class);
    
    private final YdbConnector yc;
    private final ExporterJob job;

    private final AtomicBoolean shouldRun = new AtomicBoolean(false);
    private final AtomicReference<ExecutorService> es = new AtomicReference<>();

    public App(YdbConnector yc, ExporterJob job) {
        this.yc = yc;
        this.job = job;
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
        if (shouldRun.get()) {
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
    }
    
    private void initExecutors() {
        ExecutorService old = es.getAndSet(
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
        if (old!=null) {
            old.shutdownNow();
            throw new IllegalStateException("Concurrent initialization");
        }
    }
    
    private void mainPagedRead() {
        LOG.info("Performing paged reads for the main query.");
        
    }
    
    private void mainSingleRead() {
        LOG.info("Performing single-action read for the main query.");
        try (QuerySession qs = yc.createQuerySession()) {
            qs.createQuery(job.getMainQuery(), TxMode.SNAPSHOT_RO)
                    .execute(part -> handleMainPart(part))
                    .join()
                    .getStatus()
                    .expectSuccess("Main query failed");
        }
        LOG.info("Main query completed.");
    }

    private void handleMainPart(QueryResultPart part) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("USAGE: App connection.xml job.xml");
            System.exit(1);
        }
        try {
            YdbConnector.Config ycc = YdbConnector.Config.fromFile(args[0]);
            ExporterJob job = ExporterJob.fromXml(args[1]);
            try (YdbConnector yc = new YdbConnector(ycc)) {
                try (App app = new App(yc, job)) {
                    app.run();
                }
            }
        } catch(Exception ex) {
            LOG.error("FATAL", ex);
        }
    }

}
