package com.jnks.iot.rule.engine.action;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotAssignToCustomerNodeConfiguration extends JnksIotAbstractCustomerActionNodeConfiguration implements NodeConfiguration<JnksIotAssignToCustomerNodeConfiguration> {

    private boolean createCustomerIfNotExists;

    @Override
    public JnksIotAssignToCustomerNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotAssignToCustomerNodeConfiguration();
        configuration.setCustomerNamePattern("");
        configuration.setCreateCustomerIfNotExists(false);
        return configuration;
    }
}
