package com.jnks.iot.rule.engine.action;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotUnassignFromCustomerNodeConfiguration extends JnksIotAbstractCustomerActionNodeConfiguration implements NodeConfiguration<JnksIotUnassignFromCustomerNodeConfiguration> {

    @Override
    public JnksIotUnassignFromCustomerNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotUnassignFromCustomerNodeConfiguration();
        configuration.setCustomerNamePattern("");
        return configuration;
    }
}
