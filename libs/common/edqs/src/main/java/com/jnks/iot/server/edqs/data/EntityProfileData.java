package com.jnks.iot.server.edqs.data;

import lombok.ToString;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.EntityFields;

import java.util.UUID;

@ToString(callSuper = true)
public class EntityProfileData extends BaseEntityData<EntityFields> {

    private final EntityType entityType;

    public EntityProfileData(UUID entityId, EntityType entityType) {
        super(entityId);
        this.entityType = entityType;
    }

    @Override
    public EntityType getEntityType() {
        return entityType;
    }

}
