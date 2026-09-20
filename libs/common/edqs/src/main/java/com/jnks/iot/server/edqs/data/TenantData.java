package com.jnks.iot.server.edqs.data;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.TenantFields;

import java.util.UUID;

public class TenantData extends BaseEntityData<TenantFields> {

    public TenantData(UUID entityId) {
        super(entityId);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.TENANT;
    }

}
