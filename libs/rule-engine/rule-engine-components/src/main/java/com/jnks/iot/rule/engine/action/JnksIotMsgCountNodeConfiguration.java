package com.jnks.iot.rule.engine.action;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
public class JnksIotMsgCountNodeConfiguration implements NodeConfiguration<JnksIotMsgCountNodeConfiguration> {

    private String telemetryPrefix;
    private int interval;

    @Override
    public JnksIotMsgCountNodeConfiguration defaultConfiguration() {
        JnksIotMsgCountNodeConfiguration configuration = new JnksIotMsgCountNodeConfiguration();
        configuration.setInterval(1);
        configuration.setTelemetryPrefix("messageCount");
        return configuration;
    }
}
