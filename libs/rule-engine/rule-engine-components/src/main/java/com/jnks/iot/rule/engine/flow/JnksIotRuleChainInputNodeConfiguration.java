package com.jnks.iot.rule.engine.flow;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
public class JnksIotRuleChainInputNodeConfiguration implements NodeConfiguration<JnksIotRuleChainInputNodeConfiguration> {

    private String ruleChainId;
    private boolean forwardMsgToDefaultRuleChain;

    @Override
    public JnksIotRuleChainInputNodeConfiguration defaultConfiguration() {
        JnksIotRuleChainInputNodeConfiguration configuration = new JnksIotRuleChainInputNodeConfiguration();
        configuration.setForwardMsgToDefaultRuleChain(false);
        return configuration;
    }

}
