package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;

import java.util.Collections;

@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotGetCustomerDetailsNodeConfiguration extends JnksIotAbstractGetEntityDetailsNodeConfiguration implements NodeConfiguration<JnksIotGetCustomerDetailsNodeConfiguration> {

    @Override
    public JnksIotGetCustomerDetailsNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotGetCustomerDetailsNodeConfiguration();
        configuration.setDetailsList(Collections.emptyList());
        configuration.setFetchTo(JnksIotMsgSource.DATA);
        return configuration;
    }

}
