package com.jnks.iot.server.service.cf.ctx.state;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.script.api.tbel.TbelCfArg;
import com.jnks.iot.script.api.tbel.TbelCfSingleValueArg;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BasicKvEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.util.ProtoUtils;
import com.jnks.iot.server.gen.transport.TransportProtos.AttributeValueProto;
import com.jnks.iot.server.gen.transport.TransportProtos.TsKvProto;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SingleValueArgumentEntry implements ArgumentEntry {

    private long ts;
    private BasicKvEntry kvEntryValue;
    private Long version;

    private boolean forceResetPrevious;

    public SingleValueArgumentEntry(TsKvProto entry) {
        this.ts = entry.getTs();
        if (entry.hasVersion()) {
            this.version = entry.getVersion();
        }
        this.kvEntryValue = ProtoUtils.fromProto(entry.getKv());
    }

    public SingleValueArgumentEntry(AttributeValueProto entry) {
        this.ts = entry.getLastUpdateTs();
        if (entry.hasVersion()) {
            this.version = entry.getVersion();
        }
        this.kvEntryValue = ProtoUtils.basicKvEntryFromProto(entry);
    }

    public SingleValueArgumentEntry(KvEntry entry) {
        if (entry instanceof TsKvEntry tsKvEntry) {
            this.ts = tsKvEntry.getTs();
            this.version = tsKvEntry.getVersion();
        } else if (entry instanceof AttributeKvEntry attributeKvEntry) {
            this.ts = attributeKvEntry.getLastUpdateTs();
            this.version = attributeKvEntry.getVersion();
        }
        this.kvEntryValue = ProtoUtils.basicKvEntryFromKvEntry(entry);
    }

    public SingleValueArgumentEntry(long ts, BasicKvEntry kvEntryValue, Long version) {
        this.ts = ts;
        this.kvEntryValue = kvEntryValue;
        this.version = version;
    }

    @Override
    public ArgumentEntryType getType() {
        return ArgumentEntryType.SINGLE_VALUE;
    }

    @Override
    public boolean isEmpty() {
        return kvEntryValue == null;
    }

    @JsonIgnore
    public Object getValue() {
        return isEmpty() ? null : kvEntryValue.getValue();
    }

    @Override
    public TbelCfArg toTbelCfArg() {
        return new TbelCfSingleValueArg(ts, kvEntryValue.getValue());
    }

    @Override
    public boolean updateEntry(ArgumentEntry entry) {
        if (entry instanceof SingleValueArgumentEntry singleValueEntry) {
            if (singleValueEntry.getTs() == this.ts) {
                return false;
            }

            Long newVersion = singleValueEntry.getVersion();
            if (newVersion == null || this.version == null || newVersion > this.version) {
                this.ts = singleValueEntry.getTs();
                this.version = newVersion;
                this.kvEntryValue = singleValueEntry.getKvEntryValue();
                return true;
            }
        } else {
            throw new IllegalArgumentException("Unsupported argument entry type for single value argument entry: " + entry.getType());
        }
        return false;
    }
}
