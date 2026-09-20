package com.jnks.iot.server.edqs.data.dp;

import lombok.Getter;
import com.jnks.iot.server.common.data.edqs.DataPoint;
import com.jnks.iot.server.common.data.kv.DataType;

public class BoolDataPoint extends AbstractDataPoint {

    @Getter
    private final boolean value;

    public BoolDataPoint(long ts, boolean value) {
        super(ts);
        this.value = value;
    }

    @Override
    public DataType getType() {
        return DataType.BOOLEAN;
    }

    @Override
    public boolean getBool() {
        return value;
    }

    @Override
    public String valueToString() {
        return Boolean.toString(value);
    }

    @Override
    public int compareTo(DataPoint dataPoint) {
        if (dataPoint.getType() == DataType.BOOLEAN) {
            return Boolean.compare(value, dataPoint.getBool());
        } else {
            return super.compareTo(dataPoint);
        }
    }
}
