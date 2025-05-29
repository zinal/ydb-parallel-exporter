package tech.ydb.samples.exporter;

import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author mzinal
 */
public class ValueConvertor {
    
    public static String[] convertRecord(ResultSetReader output) {
        String[] values = new String[output.getColumnCount()];
        for (int i = 0; i < output.getColumnCount(); ++i) {
            values[i] = convert(output.getColumn(i));
        }
        return values;
    }

    public static String convert(ValueReader vr) {
        if (vr==null) {
            return "";
        }
        Value<?> value = vr.getValue();
        if (value instanceof OptionalValue ov) {
            if (ov.isPresent()) {
                value = ov.get();
            } else {
                return "";
            }
        }
        if (value==null) {
            return "";
        }
        if (value instanceof DecimalValue dv) {
            return dv.toUnscaledString();
        }
        return value.toString();
    }
    
}
