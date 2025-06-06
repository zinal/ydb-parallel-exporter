package tech.ydb.samples.exporter;

import java.io.FileInputStream;
import java.util.Properties;

/**
 *
 * @author mzinal
 */
public class App {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        if (args.length != 2 && args.length != 3) {
            System.out.println("USAGE: tech.ydb.samples.exporter.App connection.xml job.xml [properties.xml]");
            System.exit(1);
        }
        try {
            YdbConnector.Config ycc = YdbConnector.Config.fromFile(args[0]);
            JobDef job;
            if (args.length==2) {
                job = JobDef.fromXml(args[1]);
            } else {
                Properties props = new Properties();
                try (FileInputStream fis = new FileInputStream(args[2])) {
                    props.loadFromXML(fis);
                }
                job = JobDef.fromXml(args[1], props);
            }
            LOG.info("Initializing...");
            try (YdbConnector yc = new YdbConnector(ycc)) {
                try (Tool app = new Tool(yc, job)) {
                    LOG.info("Starting...");
                    app.run();
                }
                LOG.info("Finished!");
            }
        } catch(Exception ex) {
            LOG.error("Execution failed", ex);
        }
    }

}
