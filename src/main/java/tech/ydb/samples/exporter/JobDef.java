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
public class JobDef implements Serializable {

    private int workerCount = 1;
    private boolean useMainQueryPaging = false;
    private String mainQuery = null;
    private String subQuery = null;
    private Format outputFormat = Format.CSV;
    private String outputFile = "-"; // stdout
    
    public JobDef() {
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
        return fromXml(new FileInputStream(fname));
    }
    
    public static JobDef fromXml(byte[] input) throws IOException {
        return fromXml(new ByteArrayInputStream(input));
    }
    
    public static JobDef fromXml(InputStream input) throws IOException {
        return new JobDef();
    }
    
    public static enum Format {
        CSV,
        TSV,
        JSON
    }
    
}
