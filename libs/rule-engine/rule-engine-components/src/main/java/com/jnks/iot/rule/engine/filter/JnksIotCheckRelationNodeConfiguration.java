package com.jnks.iot.rule.engine.filter;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;

/**
 * Created by ashvayka on 19.01.18.
 */
@Data
public class JnksIotCheckRelationNodeConfiguration implements NodeConfiguration<JnksIotCheckRelationNodeConfiguration> {

    private String direction;
    private String entityId;
    private String entityType;
    private String relationType;
    private boolean checkForSingleEntity;

    @Override
    public JnksIotCheckRelationNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotCheckRelationNodeConfiguration();
        configuration.setDirection(EntitySearchDirection.FROM.name());
        configuration.setRelationType(EntityRelation.CONTAINS_TYPE);
        configuration.setCheckForSingleEntity(true);
        return configuration;
    }
}
