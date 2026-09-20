package com.jnks.iot.server.common.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import com.jnks.iot.server.common.data.kv.AggTsKvEntry;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BaseAttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.BooleanDataEntry;
import com.jnks.iot.server.common.data.kv.DataType;
import com.jnks.iot.server.common.data.kv.DoubleDataEntry;
import com.jnks.iot.server.common.data.kv.JsonDataEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.kv.LongDataEntry;
import com.jnks.iot.server.common.data.kv.StringDataEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class KvProtoUtilTest {

    private static final long TS = System.currentTimeMillis();

    private static Stream<KvEntry> kvEntryData() {
        String key = "key";
        return Stream.of(
                new BooleanDataEntry(key, true),
                new LongDataEntry(key, 23L),
                new DoubleDataEntry(key, 23.0),
                new StringDataEntry(key, "stringValue"),
                new JsonDataEntry(key, "jsonValue")
        );
    }

    private static Stream<KvEntry> basicTsKvEntryData() {
        return kvEntryData().map(kvEntry -> new BasicTsKvEntry(TS, kvEntry));
    }

    private static Stream<List<BaseAttributeKvEntry>> attributeKvEntryData() {
        return Stream.of(kvEntryData().map(kvEntry -> new BaseAttributeKvEntry(TS, kvEntry)).toList());
    }

    private static List<TsKvEntry> createTsKvEntryList(boolean withAggregation) {
        return kvEntryData().map(kvEntry -> {
                    if (withAggregation) {
                        return new AggTsKvEntry(TS, kvEntry, 0);
                    } else {
                        return new BasicTsKvEntry(TS, kvEntry);
                    }
                }).collect(Collectors.toList());
    }

    @ParameterizedTest
    @EnumSource(DataType.class)
    void protoDataTypeSerialization(DataType dataType) {
        assertThat(KvProtoUtil.fromKeyValueTypeProto(KvProtoUtil.toKeyValueTypeProto(dataType)))
                .as(dataType.name()).isEqualTo(dataType);
    }

    @ParameterizedTest
    @MethodSource("kvEntryData")
    void protoKeyValueProtoSerialization(KvEntry kvEntry) {
        assertThat(KvProtoUtil.fromTsKvProto(KvProtoUtil.toKeyValueTypeProto(kvEntry)))
                .as("deserialized").isEqualTo(kvEntry);
    }

    @ParameterizedTest
    @MethodSource("basicTsKvEntryData")
    void protoTsKvEntrySerialization(KvEntry kvEntry) {
        assertThat(KvProtoUtil.fromTsKvProto(KvProtoUtil.toTsKvProto(TS, kvEntry)))
                .as("deserialized").isEqualTo(kvEntry);
    }

    @ParameterizedTest
    @MethodSource("kvEntryData")
    void protoTsValueSerialization(KvEntry kvEntry) {
        assertThat(KvProtoUtil.fromTsValueProto(kvEntry.getKey(), KvProtoUtil.toTsValueProto(TS, kvEntry)))
                .as("deserialized").isEqualTo(kvEntry);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void protoListTsKvEntrySerialization(boolean withAggregation) {
        List<TsKvEntry> tsKvEntries = createTsKvEntryList(withAggregation);
        assertThat(KvProtoUtil.fromTsKvProtoList(KvProtoUtil.toTsKvProtoList(tsKvEntries)))
                .as("deserialized").isEqualTo(tsKvEntries);
    }

    @ParameterizedTest
    @MethodSource("attributeKvEntryData")
    void protoListAttributeKvSerialization(List<AttributeKvEntry> attributeKvEntries) {
        assertThat(KvProtoUtil.toAttributeKvList(KvProtoUtil.attrToTsKvProtos(attributeKvEntries)))
                .as("deserialized")
                .isEqualTo(attributeKvEntries);
    }

}
