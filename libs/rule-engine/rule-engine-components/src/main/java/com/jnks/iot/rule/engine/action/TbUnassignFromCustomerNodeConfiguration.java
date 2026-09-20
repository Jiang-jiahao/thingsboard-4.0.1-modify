package com.jnks.iot.rule.engine.action;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
@EqualsAndHashCode(callSuper = true)
public class TbUnassignFromCustomerNodeConfiguration extends TbAbstractCustomerActionNodeConfiguration implements NodeConfiguration<TbUnassignFromCustomerNodeConfiguration> {

    @Override
    public TbUnassignFromCustomerNodeConfiguration defaultConfiguration() {
        var configuration = new TbUnassignFromCustomerNodeConfiguration();
        configuration.setCustomerNamePattern("");
        return configuration;
    }
}
