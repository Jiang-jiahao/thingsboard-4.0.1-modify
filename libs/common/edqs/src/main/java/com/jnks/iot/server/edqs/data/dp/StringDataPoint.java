package com.jnks.iot.server.edqs.data.dp;

import lombok.Getter;
import com.jnks.iot.server.common.data.kv.DataType;
import com.jnks.iot.common.util.TbStringPool;

public class StringDataPoint extends AbstractDataPoint {

    @Getter
    private final String value;

    public StringDataPoint(long ts, String value) {
        this(ts, value, true);
    }

    public StringDataPoint(long ts, String value, boolean deduplicate) {
        super(ts);
        this.value = deduplicate ? TbStringPool.intern(value) : value;
    }

    @Override
    public DataType getType() {
        return DataType.STRING;
    }

    @Override
    public String getStr() {
        return value;
    }

    @Override
    public String valueToString() {
        return value;
    }

}
