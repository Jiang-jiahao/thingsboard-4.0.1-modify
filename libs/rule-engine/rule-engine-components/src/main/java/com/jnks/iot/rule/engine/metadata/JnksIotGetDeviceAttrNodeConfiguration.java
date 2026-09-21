package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.data.DeviceRelationsQuery;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;

import java.util.Collections;

@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotGetDeviceAttrNodeConfiguration extends JnksIotGetAttributesNodeConfiguration {

    private DeviceRelationsQuery deviceRelationsQuery;

    @Override
    public JnksIotGetDeviceAttrNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotGetDeviceAttrNodeConfiguration();
        configuration.setClientAttributeNames(Collections.emptyList());
        configuration.setSharedAttributeNames(Collections.emptyList());
        configuration.setServerAttributeNames(Collections.emptyList());
        configuration.setLatestTsKeyNames(Collections.emptyList());
        configuration.setTellFailureIfAbsent(true);
        configuration.setGetLatestValueWithTs(false);
        configuration.setFetchTo(JnksIotMsgSource.METADATA);

        var deviceRelationsQuery = new DeviceRelationsQuery();
        deviceRelationsQuery.setDirection(EntitySearchDirection.FROM);
        deviceRelationsQuery.setMaxLevel(1);
        deviceRelationsQuery.setRelationType(EntityRelation.CONTAINS_TYPE);
        deviceRelationsQuery.setDeviceTypes(Collections.singletonList("default"));

        configuration.setDeviceRelationsQuery(deviceRelationsQuery);

        return configuration;
    }

}
