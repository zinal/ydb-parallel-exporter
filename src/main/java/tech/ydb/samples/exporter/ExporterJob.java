package tech.ydb.samples.exporter;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 *
 * @author mzinal
 */
public class ExporterJob implements Serializable {

    private int workerCount = 1;
    private boolean useMainQueryPaging = false;
    private String mainQuery = null;
    private String subQuery = null;
    
    public ExporterJob() {
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }

    public boolean isUseMainQueryPaging() {
        return useMainQueryPaging;
    }

    public void setUseMainQueryPaging(boolean useMainQueryPaging) {
        this.useMainQueryPaging = useMainQueryPaging;
    }

    public String getMainQuery() {
        return mainQuery;
    }

    public void setMainQuery(String mainQuery) {
        this.mainQuery = mainQuery;
    }

    public String getSubQuery() {
        return subQuery;
    }

    public void setSubQuery(String subQuery) {
        this.subQuery = subQuery;
    }

    public static ExporterJob fromXml(String fname) throws IOException {
        return fromXml(new FileInputStream(fname));
    }
    
    public static ExporterJob fromXml(byte[] input) throws IOException {
        return fromXml(new ByteArrayInputStream(input));
    }
    
    public static ExporterJob fromXml(InputStream input) throws IOException {
        return new ExporterJob();
    }
    
}
