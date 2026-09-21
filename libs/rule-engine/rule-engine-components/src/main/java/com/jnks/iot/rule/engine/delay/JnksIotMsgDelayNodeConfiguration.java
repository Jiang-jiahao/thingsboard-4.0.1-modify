package com.jnks.iot.rule.engine.delay;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
public class JnksIotMsgDelayNodeConfiguration implements NodeConfiguration<JnksIotMsgDelayNodeConfiguration> {

    private int periodInSeconds;
    private int maxPendingMsgs;
    private String periodInSecondsPattern;
    private boolean useMetadataPeriodInSecondsPatterns;

    @Override
    public JnksIotMsgDelayNodeConfiguration defaultConfiguration() {
        JnksIotMsgDelayNodeConfiguration configuration = new JnksIotMsgDelayNodeConfiguration();
        configuration.setPeriodInSeconds(60);
        configuration.setMaxPendingMsgs(1000);
        configuration.setUseMetadataPeriodInSecondsPatterns(false);
        return configuration;
    }
}
