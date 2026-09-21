package com.jnks.iot.rule.engine.transform;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.data.RelationsQuery;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.data.relation.RelationEntityTypeFilter;

import java.util.Collections;

import static com.jnks.iot.rule.engine.transform.OriginatorSource.CUSTOMER;

@Data
public class JnksIotChangeOriginatorNodeConfiguration implements NodeConfiguration<JnksIotChangeOriginatorNodeConfiguration> {

    private OriginatorSource originatorSource;
    private RelationsQuery relationsQuery;
    private String entityType;
    private String entityNamePattern;

    @Override
    public JnksIotChangeOriginatorNodeConfiguration defaultConfiguration() {
        JnksIotChangeOriginatorNodeConfiguration configuration = new JnksIotChangeOriginatorNodeConfiguration();
        configuration.setOriginatorSource(CUSTOMER);

        RelationsQuery relationsQuery = new RelationsQuery();
        relationsQuery.setDirection(EntitySearchDirection.FROM);
        relationsQuery.setMaxLevel(1);
        RelationEntityTypeFilter relationEntityTypeFilter = new RelationEntityTypeFilter(EntityRelation.CONTAINS_TYPE, Collections.emptyList());
        relationsQuery.setFilters(Collections.singletonList(relationEntityTypeFilter));
        configuration.setRelationsQuery(relationsQuery);

        return configuration;
    }
}
