package com.jnks.iot.rule.engine.action;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;

@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotCreateRelationNodeConfiguration extends JnksIotAbstractRelationActionNodeConfiguration implements NodeConfiguration<JnksIotCreateRelationNodeConfiguration> {

    private boolean createEntityIfNotExists;
    private boolean changeOriginatorToRelatedEntity;
    private boolean removeCurrentRelations;

    @Override
    public JnksIotCreateRelationNodeConfiguration defaultConfiguration() {
        JnksIotCreateRelationNodeConfiguration configuration = new JnksIotCreateRelationNodeConfiguration();
        configuration.setDirection(EntitySearchDirection.FROM);
        configuration.setRelationType(EntityRelation.CONTAINS_TYPE);
        configuration.setEntityNamePattern("");
        configuration.setCreateEntityIfNotExists(false);
        configuration.setRemoveCurrentRelations(false);
        configuration.setChangeOriginatorToRelatedEntity(false);
        return configuration;
    }
}
