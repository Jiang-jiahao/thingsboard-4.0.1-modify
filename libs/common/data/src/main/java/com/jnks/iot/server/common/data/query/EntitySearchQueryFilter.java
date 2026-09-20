package com.jnks.iot.server.common.data.query;

import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;

@Data
public abstract class EntitySearchQueryFilter implements EntityFilter {

    private EntityId rootEntity;
    private String relationType;
    private EntitySearchDirection direction;
    private int maxLevel;
    private boolean fetchLastLevelOnly;

}
