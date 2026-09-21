package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.data.RelationsQuery;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.data.relation.RelationEntityTypeFilter;

import java.util.Collections;
import java.util.HashMap;

@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotGetRelatedDataNodeConfiguration extends JnksIotGetEntityDataNodeConfiguration {

    private RelationsQuery relationsQuery;

    @Override
    public JnksIotGetRelatedDataNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotGetRelatedDataNodeConfiguration();
        var dataMapping = new HashMap<String, String>();
        dataMapping.putIfAbsent("serialNumber", "sn");
        configuration.setDataMapping(dataMapping);
        configuration.setDataToFetch(DataToFetch.ATTRIBUTES);
        configuration.setFetchTo(JnksIotMsgSource.METADATA);

        var relationsQuery = new RelationsQuery();
        var relationEntityTypeFilter = new RelationEntityTypeFilter(EntityRelation.CONTAINS_TYPE, Collections.emptyList());
        relationsQuery.setDirection(EntitySearchDirection.FROM);
        relationsQuery.setMaxLevel(1);
        relationsQuery.setFilters(Collections.singletonList(relationEntityTypeFilter));
        configuration.setRelationsQuery(relationsQuery);

        return configuration;
    }

}
