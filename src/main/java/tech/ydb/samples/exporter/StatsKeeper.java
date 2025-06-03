package tech.ydb.samples.exporter;

import java.util.ArrayList;
import tech.ydb.table.values.Value;

/**
 * Statistics helper.
 *
 * @author zinal
 */
class StatsKeeper {
    
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(StatsKeeper.class);
    
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

    synchronized void updateInput(RowSet datum) {
        rowsInput += datum.getRowCount();
        reportProgressIf();
    }

    synchronized void updateInside(Value<?>[] block) {
        rowsInside += block.length;
        reportProgressIf();
    }

    synchronized void updateOutput(ArrayList<Object[]> block) {
        // minus one, because first row contains column names
        rowsOutput += block.size() - 1;
        reportProgressIf();
    }

    synchronized void reportProgress() {
        long tv = System.currentTimeMillis();
        printStats(tv);
        saveState(tv);
    }

    private void saveState(long tv) {
        tvLast = tv;
        rowsInputPrev = rowsInput;
        rowsInsidePrev = rowsInside;
        rowsOutputPrev = rowsOutput;
    }

    private void reportProgressIf() {
        long tv = System.currentTimeMillis();
        if (tv - tvLast >= 10000L) {
            printStats(tv);
            saveState(tv);
        }
    }

    private void printStats(long tv) {
        long input = rowsInput - rowsInputPrev;
        long inside = rowsInside - rowsInsidePrev;
        long output = rowsOutput - rowsOutputPrev;
        long millis = tv - tvLast;
        double rateInput = ((double) input) * 1000.0 / ((double) millis);
        double rateInside = ((double) inside) * 1000.0 / ((double) millis);
        double rateOutput = ((double) output) * 1000.0 / ((double) millis);
        LOG.info("Total {} input, {} output rows. (in/bw/out) {}/{}/{} rows, {}/{}/{} rps.",
                rowsInput, rowsOutput, input, inside, output,
                String.format("%.2f", rateInput),
                String.format("%.2f", rateInside),
                String.format("%.2f", rateOutput));
    }

}
