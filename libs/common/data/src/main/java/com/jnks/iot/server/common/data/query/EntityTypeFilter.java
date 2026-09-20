package com.jnks.iot.server.common.data.query;

import lombok.Data;
import com.jnks.iot.server.common.data.EntityType;

@Data
public class EntityTypeFilter implements EntityFilter {
    @Override
    public EntityFilterType getType() {
        return EntityFilterType.ENTITY_TYPE;
    }

    private EntityType entityType;

}
