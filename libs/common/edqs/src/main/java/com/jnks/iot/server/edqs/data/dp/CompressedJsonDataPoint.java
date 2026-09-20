package com.jnks.iot.server.edqs.data.dp;

import com.jnks.iot.server.common.data.kv.DataType;

import java.util.function.Function;

public class CompressedJsonDataPoint extends CompressedStringDataPoint {

    public CompressedJsonDataPoint(long ts, byte[] compressedValue, Function<byte[], String> uncompressor) {
        super(ts, compressedValue, uncompressor);
    }

    @Override
    public DataType getType() {
        return DataType.JSON;
    }

}
