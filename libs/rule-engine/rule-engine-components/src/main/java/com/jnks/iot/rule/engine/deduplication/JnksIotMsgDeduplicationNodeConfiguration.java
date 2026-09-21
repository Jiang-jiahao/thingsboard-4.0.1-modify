package com.jnks.iot.rule.engine.deduplication;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
public class JnksIotMsgDeduplicationNodeConfiguration implements NodeConfiguration<JnksIotMsgDeduplicationNodeConfiguration> {

    private int interval;
    private DeduplicationStrategy strategy;

    // only for DeduplicationStrategy.ALL:
    private String outMsgType;

    // Advanced settings:
    private int maxPendingMsgs;
    private int maxRetries;

    @Override
    public JnksIotMsgDeduplicationNodeConfiguration defaultConfiguration() {
        JnksIotMsgDeduplicationNodeConfiguration configuration = new JnksIotMsgDeduplicationNodeConfiguration();
        configuration.setInterval(60);
        configuration.setStrategy(DeduplicationStrategy.FIRST);
        configuration.setMaxPendingMsgs(100);
        configuration.setMaxRetries(3);
        return configuration;
    }
}
