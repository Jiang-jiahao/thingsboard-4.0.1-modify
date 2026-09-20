package com.jnks.iot.server.common.data.edqs;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.ObjectType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AttributeKv implements EdqsObject {

    private EntityId entityId;
    private AttributeScope scope;
    private String key;
    private Long version;

    private DataPoint dataPoint; // optional (on deletion)

    private Long lastUpdateTs; // only for serialization
    private KvEntry value; // only for serialization

    public AttributeKv(EntityId entityId, AttributeScope scope, AttributeKvEntry attributeKvEntry, long version) {
        this.entityId = entityId;
        this.scope = scope;
        this.key = attributeKvEntry.getKey();
        this.version = version;
        this.lastUpdateTs = attributeKvEntry.getLastUpdateTs();
        this.value = attributeKvEntry;
    }

    public AttributeKv(EntityId entityId, AttributeScope scope, String key, long version) {
        this.entityId = entityId;
        this.scope = scope;
        this.key = key;
        this.version = version;
    }

    @Override
    public String key() {
        return "a_" + entityId + "_" + scope + "_" + key;
    }

    @Override
    public Long version() {
        return version;
    }

    @Override
    public ObjectType type() {
        return ObjectType.ATTRIBUTE_KV;
    }

}
