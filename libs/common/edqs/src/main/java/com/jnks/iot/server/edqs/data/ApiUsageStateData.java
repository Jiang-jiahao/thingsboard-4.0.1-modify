package com.jnks.iot.server.edqs.data;

import lombok.ToString;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.ApiUsageStateFields;

import java.util.UUID;

@ToString(callSuper = true)
public class ApiUsageStateData extends BaseEntityData<ApiUsageStateFields> {

    public ApiUsageStateData(UUID entityId) {
        super(entityId);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.API_USAGE_STATE;
    }

    @Override
    public String getEntityName() {
        return getOwnerName();
    }

    @Override
    public String getOwnerName() {
        return repo.getOwnerEntityName(fields.getEntityId());
    }

}
