package com.jnks.iot.rule.engine.rpc;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
public class JnksIotSendRpcRequestNodeConfiguration implements NodeConfiguration<JnksIotSendRpcRequestNodeConfiguration> {

    private int timeoutInSeconds;

    @Override
    public JnksIotSendRpcRequestNodeConfiguration defaultConfiguration() {
        JnksIotSendRpcRequestNodeConfiguration configuration = new JnksIotSendRpcRequestNodeConfiguration();
        configuration.setTimeoutInSeconds(60);
        return configuration;
    }
}
