package tech.ydb.samples.exporter;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import org.jdom2.Element;

/**
 *
 * @author mzinal
 */
public class JobDef implements Serializable {

    private int workerCount = 1;
    private String mainQuery = null;
    private String pageQuery = null;
    private final ArrayList<String> pageInput = new ArrayList<>();
    private String detailsQuery = null;
    private final ArrayList<String> detailsInput = new ArrayList<>();
    private Format outputFormat = Format.CSV;
    private String outputFile = "-"; // stdout
    
    public JobDef() {
    }
    
    public boolean hasPageQuery() {
        return pageQuery!=null && !pageQuery.isEmpty();
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
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

    public static JobDef fromXml(String fname) throws IOException {
        JobDef job = new JobDef();
        Element docRoot = JdomHelper.readDocument(fname);
        Element el;
        el = JdomHelper.getOneChild(docRoot, "worker-count");
        if (el!=null) {
            job.setWorkerCount(JdomHelper.getInt(el));
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
        CSV,
        TSV,
        JSON
    }
    
}
