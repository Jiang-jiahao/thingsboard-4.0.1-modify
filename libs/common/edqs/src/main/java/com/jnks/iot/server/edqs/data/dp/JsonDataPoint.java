package com.jnks.iot.server.edqs.data.dp;

import lombok.Getter;
import com.jnks.iot.server.common.data.kv.DataType;
import com.jnks.iot.common.util.JnksIotStringPool;

public class JsonDataPoint extends AbstractDataPoint {

    @Getter
    private final String value;

    public JsonDataPoint(long ts, String value) {
        super(ts);
        this.value = JnksIotStringPool.intern(value);
    }

    @Override
    public DataType getType() {
        return DataType.JSON;
    }

    @Override
    public String getJson() {
        return value;
    }

    @Override
    public String valueToString() {
        return value;
    }

}
