package com.jnks.iot.server.edqs.query;

import lombok.Data;
import com.jnks.iot.server.common.data.edqs.DataPoint;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.edqs.data.EntityData;

import java.util.UUID;

@Data
public class SortableEntityData {

    private final EntityData entityData;
    private DataPoint sortValue;

    public UUID getId(){
        return entityData.getId();
    }

    public EntityId getEntityId() {
        return EntityIdFactory.getByTypeAndUuid(entityData.getEntityType(), entityData.getId());
    }
}
