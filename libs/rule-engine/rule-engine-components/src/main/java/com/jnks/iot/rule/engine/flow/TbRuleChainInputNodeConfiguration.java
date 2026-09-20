package com.jnks.iot.rule.engine.flow;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
public class TbRuleChainInputNodeConfiguration implements NodeConfiguration<TbRuleChainInputNodeConfiguration> {

    private String ruleChainId;
    private boolean forwardMsgToDefaultRuleChain;

    @Override
    public TbRuleChainInputNodeConfiguration defaultConfiguration() {
        TbRuleChainInputNodeConfiguration configuration = new TbRuleChainInputNodeConfiguration();
        configuration.setForwardMsgToDefaultRuleChain(false);
        return configuration;
    }

}
