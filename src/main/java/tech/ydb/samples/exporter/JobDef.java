package tech.ydb.samples.exporter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import org.apache.commons.text.StringSubstitutor;
import org.jdom2.Element;
import tech.ydb.common.transaction.TxMode;

/**
 * Batch record processor job definition.
 * 
 * @author zinal
 */
public class JobDef implements Serializable {

    private int workerCount = 1;
    private int queueSize = 100;
    private TxMode isolation = null;
    private String mainQuery = null;
    private String pageQuery = null;
    private final ArrayList<String> pageInput = new ArrayList<>();
    private String detailsQuery = null;
    private final ArrayList<String> detailsInput = new ArrayList<>();
    private int detailsBatchLimit = -1;
    private Format outputFormat = Format.CSV;
    private String outputFile = "-"; // stdout
    private String outputEncoding = null;
    
    public JobDef() {
    }

    /**
     * @return true, if pageQuery is defined, and false otherwise
     */
    public boolean hasPageQuery() {
        return pageQuery!=null && !pageQuery.isEmpty();
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public TxMode getIsolation() {
        return isolation;
    }

    public void setIsolation(TxMode isolation) {
        this.isolation = isolation;
    }

    public String getMainQuery() {
        return mainQuery;
    }

    public void setMainQuery(String mainQuery) {
        this.mainQuery = mainQuery;
    }

    public String getPageQuery() {
        return pageQuery;
    }

    public void setPageQuery(String pageQuery) {
        this.pageQuery = pageQuery;
    }

    public ArrayList<String> getPageInput() {
        return pageInput;
    }

    public String getDetailsQuery() {
        return detailsQuery;
    }

    public void setDetailsQuery(String subQuery) {
        this.detailsQuery = subQuery;
    }

    public ArrayList<String> getDetailsInput() {
        return detailsInput;
    }

    public int getDetailsBatchLimit() {
        return detailsBatchLimit;
    }

    public void setDetailsBatchLimit(int detailsBatchLimit) {
        this.detailsBatchLimit = detailsBatchLimit;
    }

    public Format getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(Format outputFormat) {
        this.outputFormat = outputFormat;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public String getOutputEncoding() {
        return outputEncoding;
    }

    public void setOutputEncoding(String outputEncoding) {
        this.outputEncoding = outputEncoding;
    }

    public static JobDef fromXml(String fname) throws IOException {
        return fromXml(JdomHelper.readDocument(fname));
    }

    public static JobDef fromXml(String fname, Properties props) throws IOException {
        if (props==null || props.isEmpty()) {
            return fromXml(fname);
        }
        Element elRoot = JdomHelper.readDocument(fname);
        elRoot = JdomExpander.expand(elRoot, props);
        return fromXml(elRoot);
    }

    public static JobDef fromXml(Element docRoot) {
        JobDef job = new JobDef();
        Element el;
        el = JdomHelper.getOneChild(docRoot, "worker-count");
        if (el!=null) {
            job.setWorkerCount(JdomHelper.getInt(el));
        }
        el = JdomHelper.getOneChild(docRoot, "queue-size");
        if (el!=null) {
            job.setQueueSize(JdomHelper.getInt(el));
        }
        el = JdomHelper.getOneChild(docRoot, "batch-limit");
        if (el!=null) {
            job.setDetailsBatchLimit(JdomHelper.getInt(el));
        }
        el = JdomHelper.getOneChild(docRoot, "isolation");
        if (el!=null) {
            String v = JdomHelper.getText(el);
            for (TxMode m : TxMode.values()) {
                if (m.name().equalsIgnoreCase(v)) {
                    job.setIsolation(m);
                }
            }
            if (job.getIsolation()==null) {
                throw JdomHelper.raise(el, "Unknown isolation mode: " + v);
            }
        }
        el = JdomHelper.getOneChild(docRoot, "output-format");
        if (el!=null) {
            String t = JdomHelper.getText(el);
            boolean found = false;
            for (Format f : Format.values()) {
                if (f.name().equalsIgnoreCase(t)) {
                    found = true;
                    job.setOutputFormat(f);
                    break;
                }
            }
            if (!found) {
                throw JdomHelper.raise(el, "Illegal output format value: " + t);
            }
        }
        el = JdomHelper.getOneChild(docRoot, "output-file");
        if (el!=null) {
            job.setOutputFile(JdomHelper.getText(el));
        }
        el = JdomHelper.getOneChild(docRoot, "output-encoding");
        if (el!=null) {
            job.setOutputEncoding(JdomHelper.getText(el));
        }
        job.setMainQuery(JdomHelper.getText(docRoot, "query-main"));
        Element elPageQuery = JdomHelper.getOneChild(docRoot, "query-page");
        if (elPageQuery != null) {
            job.setPageQuery(JdomHelper.getText(elPageQuery));
        }
        job.setDetailsQuery(JdomHelper.getText(docRoot, "query-details"));
        el = JdomHelper.getOneChild(docRoot, "input-page");
        if (el!=null) {
            JdomHelper.getSomeChildren(el, "column-name")
                    .forEach(col -> job.getPageInput().add(JdomHelper.getText(col)));
        }
        if (job.hasPageQuery() && job.getPageInput().isEmpty()) {
            throw JdomHelper.raise(elPageQuery, "Missing input columns for page query");
        }
        el = JdomHelper.getOneChild(docRoot, "input-details");
        if (el!=null) {
            JdomHelper.getSomeChildren(el, "column-name")
                    .forEach(col -> job.getDetailsInput().add(JdomHelper.getText(col)));
        }
        if (job.getDetailsInput().isEmpty()) {
            throw JdomHelper.raise(docRoot, "Missing input columns for details query");
        }
        return job;
    }
    
    public static enum Format {
        CUSTOM1,
        CSV,
        TSV,
        JSON
    }
    
}
