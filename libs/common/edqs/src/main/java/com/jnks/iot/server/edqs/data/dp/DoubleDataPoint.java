package com.jnks.iot.server.edqs.data.dp;

import lombok.Getter;
import com.jnks.iot.server.common.data.edqs.DataPoint;
import com.jnks.iot.server.common.data.kv.DataType;

public class DoubleDataPoint extends AbstractDataPoint {

    @Getter
    private final double value;

    public DoubleDataPoint(long ts, double value) {
        super(ts);
        this.value = value;
    }

    @Override
    public DataType getType() {
        return DataType.DOUBLE;
    }

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public String valueToString() {
        return Double.toString(value);
    }

    @Override
    public int compareTo(DataPoint dataPoint) {
        if (dataPoint.getType() == DataType.DOUBLE || dataPoint.getType() == DataType.LONG) {
            return Double.compare(value, dataPoint.getDouble());
        } else {
            return super.compareTo(dataPoint);
        }
    }
}
