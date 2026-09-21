package com.jnks.iot.rule.engine.action;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;

@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotDeleteRelationNodeConfiguration extends JnksIotAbstractRelationActionNodeConfiguration implements NodeConfiguration<JnksIotDeleteRelationNodeConfiguration> {

    private boolean deleteForSingleEntity;

    @Override
    public JnksIotDeleteRelationNodeConfiguration defaultConfiguration() {
        JnksIotDeleteRelationNodeConfiguration configuration = new JnksIotDeleteRelationNodeConfiguration();
        configuration.setDeleteForSingleEntity(false);
        configuration.setDirection(EntitySearchDirection.FROM);
        configuration.setRelationType(EntityRelation.CONTAINS_TYPE);
        configuration.setEntityNamePattern("");
        return configuration;
    }
}
