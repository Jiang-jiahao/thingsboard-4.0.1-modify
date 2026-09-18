package org.thingsboard.rule.engine.action;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.thingsboard.rule.engine.api.NodeConfiguration;

@Data
@EqualsAndHashCode(callSuper = true)
public class TbAssignToCustomerNodeConfiguration extends TbAbstractCustomerActionNodeConfiguration implements NodeConfiguration<TbAssignToCustomerNodeConfiguration> {

    private boolean createCustomerIfNotExists;

    @Override
    public TbAssignToCustomerNodeConfiguration defaultConfiguration() {
        var configuration = new TbAssignToCustomerNodeConfiguration();
        configuration.setCustomerNamePattern("");
        configuration.setCreateCustomerIfNotExists(false);
        return configuration;
    }
}
