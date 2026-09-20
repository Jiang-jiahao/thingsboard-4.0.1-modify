package com.jnks.iot.server.common.data.edqs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.ObjectType;
import com.jnks.iot.server.common.data.edqs.fields.EntityFields;
import com.jnks.iot.server.common.data.edqs.fields.EntityIdFields;

import java.util.UUID;

@Data
@NoArgsConstructor
public class Entity implements EdqsObject {

    private EntityType type;

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    private EntityFields fields;

    public Entity(EntityType type) {
        this.type = type;
    }

    public Entity(EntityType type, EntityFields fields) {
        this.type = type;
        this.fields = fields;
    }

    public Entity(EntityType entityType, UUID id, long version) {
        this.type = entityType;
        this.fields = new EntityIdFields(id, version);
    }

    @Override
    public String key() {
        return "e_" + fields.getId().toString();
    }

    @Override
    public Long version() {
        return fields.getVersion();
    }

    @Override
    public ObjectType type() {
        return ObjectType.fromEntityType(type);
    }

}
