package com.jnks.iot.rule.engine.action;

import lombok.Data;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;

@Data
public abstract class JnksIotAbstractRelationActionNodeConfiguration {

    private EntitySearchDirection direction;
    private String relationType;

    private EntityType entityType;
    private String entityNamePattern;
    private String entityTypePattern;

}
