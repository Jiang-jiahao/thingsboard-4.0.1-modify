package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.data.DeviceRelationsQuery;
import com.jnks.iot.rule.engine.util.TbMsgSource;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;

import java.util.Collections;

@Data
@EqualsAndHashCode(callSuper = true)
public class TbGetDeviceAttrNodeConfiguration extends TbGetAttributesNodeConfiguration {

    private DeviceRelationsQuery deviceRelationsQuery;

    @Override
    public TbGetDeviceAttrNodeConfiguration defaultConfiguration() {
        var configuration = new TbGetDeviceAttrNodeConfiguration();
        configuration.setClientAttributeNames(Collections.emptyList());
        configuration.setSharedAttributeNames(Collections.emptyList());
        configuration.setServerAttributeNames(Collections.emptyList());
        configuration.setLatestTsKeyNames(Collections.emptyList());
        configuration.setTellFailureIfAbsent(true);
        configuration.setGetLatestValueWithTs(false);
        configuration.setFetchTo(TbMsgSource.METADATA);

        var deviceRelationsQuery = new DeviceRelationsQuery();
        deviceRelationsQuery.setDirection(EntitySearchDirection.FROM);
        deviceRelationsQuery.setMaxLevel(1);
        deviceRelationsQuery.setRelationType(EntityRelation.CONTAINS_TYPE);
        deviceRelationsQuery.setDeviceTypes(Collections.singletonList("default"));

        configuration.setDeviceRelationsQuery(deviceRelationsQuery);

        return configuration;
    }

}
