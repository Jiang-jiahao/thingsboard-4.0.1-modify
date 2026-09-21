package com.jnks.iot.rule.engine.filter;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.EntityType;

import java.util.Arrays;
import java.util.List;

@Data
public class JnksIotOriginatorTypeFilterNodeConfiguration implements NodeConfiguration<JnksIotOriginatorTypeFilterNodeConfiguration> {

    private List<EntityType> originatorTypes;

    @Override
    public JnksIotOriginatorTypeFilterNodeConfiguration defaultConfiguration() {
        JnksIotOriginatorTypeFilterNodeConfiguration configuration = new JnksIotOriginatorTypeFilterNodeConfiguration();
        configuration.setOriginatorTypes(Arrays.asList(
                EntityType.DEVICE
        ));
        return configuration;
    }
}
