package com.jnks.iot.server.service.cf.ctx.state;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.jnks.iot.script.api.tbel.TbelCfArg;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;

import java.util.List;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = SingleValueArgumentEntry.class, name = "SINGLE_VALUE"),
        @JsonSubTypes.Type(value = TsRollingArgumentEntry.class, name = "TS_ROLLING")
})
public interface ArgumentEntry {

    @JsonIgnore
    ArgumentEntryType getType();

    Object getValue();

    boolean updateEntry(ArgumentEntry entry);

    boolean isEmpty();

    TbelCfArg toTbelCfArg();

    boolean isForceResetPrevious();

    void setForceResetPrevious(boolean forceResetPrevious);

    static ArgumentEntry createSingleValueArgument(KvEntry kvEntry) {
        return new SingleValueArgumentEntry(kvEntry);
    }

    static ArgumentEntry createTsRollingArgument(List<TsKvEntry> kvEntries, int limit, long timeWindow) {
        return new TsRollingArgumentEntry(kvEntries, limit, timeWindow);
    }

}
