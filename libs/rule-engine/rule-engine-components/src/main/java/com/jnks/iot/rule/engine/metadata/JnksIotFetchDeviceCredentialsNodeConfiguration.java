package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JnksIotFetchDeviceCredentialsNodeConfiguration extends JnksIotAbstractFetchToNodeConfiguration implements NodeConfiguration<JnksIotFetchDeviceCredentialsNodeConfiguration> {

    @Override
    public JnksIotFetchDeviceCredentialsNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotFetchDeviceCredentialsNodeConfiguration();
        configuration.setFetchTo(JnksIotMsgSource.METADATA);
        return configuration;
    }

}
