package tech.ydb.samples.exporter;

import org.apache.commons.codec.binary.Base64;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.OptionalValue;
import static tech.ydb.table.values.PrimitiveType.Bool;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author mzinal
 */
public class ValueConvertor {
    
    public static Object[] convertRecord(ResultSetReader output) {
        Object[] values = new Object[output.getColumnCount()];
        for (int i = 0; i < output.getColumnCount(); ++i) {
            values[i] = convert(output.getColumn(i));
        }
        return values;
    }

    public static Object convert(ValueReader vr) {
        if (vr==null) {
            return "";
        }
        Value<?> value = vr.getValue();
        if (value instanceof OptionalValue ov) {
            if (ov.isPresent()) {
                value = ov.get();
            } else {
                return null;
            }
        }
        if (value==null) {
            return null;
        }
        if (value instanceof DecimalValue dv) {
            return dv.toUnscaledString();
        }
        if (value instanceof PrimitiveValue pv) {
            switch (pv.getType()) {
                case Bool -> {
                    return pv.getBool() ? "true" : "false";
                }
                case Text -> {
                    return pv.getText();
                }
                case Json -> {
                    return pv.getJson();
                }
                case JsonDocument -> {
                    return pv.getJsonDocument();
                }
                case Bytes -> {
                    return Base64.encodeBase64String(pv.getBytes());
                }
                case Int8 -> {
                    return (int) pv.getInt8();
                }
                case Int16 -> {
                    return (int) pv.getInt16();
                }
                case Int32 -> {
                    return pv.getInt32();
                }
                case Int64 -> {
                    return pv.getInt64();
                }
                case Uint8 -> {
                    return (int) pv.getUint8();
                }
                case Uint16 -> {
                    return (int) pv.getUint16();
                }
                case Uint32 -> {
                    return pv.getUint32();
                }
                case Uint64 -> {
                    return pv.getUint64();
                }
            }
        }
        return value.toString();
    }

}
