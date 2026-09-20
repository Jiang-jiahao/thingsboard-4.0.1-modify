package com.jnks.iot.server.common.data.query;

import lombok.Data;
import com.jnks.iot.server.common.data.EntityType;

@Data
public class EntityNameFilter implements EntityFilter {
    @Override
    public EntityFilterType getType() {
        return EntityFilterType.ENTITY_NAME;
    }

    private EntityType entityType;

    private String entityNameFilter;

}
