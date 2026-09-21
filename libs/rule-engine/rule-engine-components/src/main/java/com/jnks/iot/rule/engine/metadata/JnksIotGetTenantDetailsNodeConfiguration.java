package com.jnks.iot.rule.engine.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;

import java.util.Collections;

@Data
@EqualsAndHashCode(callSuper = true)
public class JnksIotGetTenantDetailsNodeConfiguration extends JnksIotAbstractGetEntityDetailsNodeConfiguration implements NodeConfiguration<JnksIotGetTenantDetailsNodeConfiguration> {

    @Override
    public JnksIotGetTenantDetailsNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotGetTenantDetailsNodeConfiguration();
        configuration.setDetailsList(Collections.emptyList());
        configuration.setFetchTo(JnksIotMsgSource.DATA);
        return configuration;
    }

}
