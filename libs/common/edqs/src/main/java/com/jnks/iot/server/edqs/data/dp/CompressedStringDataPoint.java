package com.jnks.iot.server.edqs.data.dp;

import lombok.Getter;
import lombok.SneakyThrows;
import com.jnks.iot.common.util.TbBytePool;
import com.jnks.iot.server.common.data.kv.DataType;

import java.util.function.Function;

public class CompressedStringDataPoint extends AbstractDataPoint {

    @Getter
    private final byte[] compressedValue;

    protected final Function<byte[], String> uncompressor;

    @SneakyThrows
    public CompressedStringDataPoint(long ts, byte[] compressedValue, Function<byte[], String> uncompressor) {
        super(ts);
        this.compressedValue = TbBytePool.intern(compressedValue);
        this.uncompressor = uncompressor;
    }

    @Override
    public DataType getType() {
        return DataType.STRING;
    }

    @SneakyThrows
    @Override
    public String getStr() {
        return uncompressor.apply(compressedValue);
    }

    @Override
    public String valueToString() {
        return getStr();
    }

}
