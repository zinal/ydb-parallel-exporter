package tech.ydb.samples.exporter;

import java.util.HashMap;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.Value;

/**
 * RowSet helper, to work around ResultSetReader limitations.
 * 
 * @author zinal
 */
class RowSet {
    
    final String[] names;
    final HashMap<String, Integer> namesMap;
    final Value<?>[][] values;

    boolean isEmpty() {
        return names.length == 0 || values.length == 0;
    }

    int getRowCount() {
        return values.length;
    }

    int getColumnCount() {
        return names.length;
    }

    int getColumnIndex(String name) {
        Integer ix = namesMap.get(name);
        if (ix == null) {
            return -1;
        }
        return ix;
    }

    Value<?> getValue(int row, int column) {
        if (row < 0 || row >= values.length) {
            throw new IllegalArgumentException("Illegal row number " + String.valueOf(row) + ", row count " + String.valueOf(values.length));
        }
        if (column < 0 || column >= names.length) {
            throw new IllegalArgumentException("Illegal column number " + String.valueOf(column) + ", column count " + String.valueOf(names.length));
        }
        return values[row][column];
    }

    Value<?> getValue(int row, String name) {
        Integer index = namesMap.get(name);
        if (index == null) {
            throw new IllegalArgumentException("Unknown column: " + name);
        }
        return getValue(row, index);
    }

    RowSet(ResultSetReader rsr) {
        this.namesMap = new HashMap<>();
        this.names = new String[rsr.getColumnCount()];
        for (int i = 0; i < rsr.getColumnCount(); ++i) {
            this.names[i] = rsr.getColumnName(i);
            this.namesMap.put(this.names[i], i);
        }
        this.values = new Value<?>[rsr.getRowCount()][];
        int row = 0;
        while (rsr.next()) {
            Value<?>[] cur = new Value<?>[this.names.length];
            for (int i = 0; i < this.names.length; ++i) {
                cur[i] = rsr.getColumn(i).getValue();
            }
            this.values[row] = cur;
            row++;
        }
    }
    
}
