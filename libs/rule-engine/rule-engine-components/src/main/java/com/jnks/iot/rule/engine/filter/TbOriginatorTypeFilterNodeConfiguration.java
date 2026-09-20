package com.jnks.iot.rule.engine.filter;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.EntityType;

import java.util.Arrays;
import java.util.List;

@Data
public class TbOriginatorTypeFilterNodeConfiguration implements NodeConfiguration<TbOriginatorTypeFilterNodeConfiguration> {

    private List<EntityType> originatorTypes;

    @Override
    public TbOriginatorTypeFilterNodeConfiguration defaultConfiguration() {
        TbOriginatorTypeFilterNodeConfiguration configuration = new TbOriginatorTypeFilterNodeConfiguration();
        configuration.setOriginatorTypes(Arrays.asList(
                EntityType.DEVICE
        ));
        return configuration;
    }
}
