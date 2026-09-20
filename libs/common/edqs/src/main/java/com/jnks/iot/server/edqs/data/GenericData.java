package com.jnks.iot.server.edqs.data;

import lombok.ToString;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.EntityFields;

import java.util.UUID;

@ToString(callSuper = true)
public class GenericData extends BaseEntityData<EntityFields> {

    private final EntityType entityType;

    public GenericData(EntityType entityType, UUID entityId) {
        super(entityId);
        this.entityType = entityType;
    }

    @Override
    public EntityType getEntityType() {
        return entityType;
    }
}
