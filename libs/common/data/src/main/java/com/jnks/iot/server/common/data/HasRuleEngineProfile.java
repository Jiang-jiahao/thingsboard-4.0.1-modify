package com.jnks.iot.server.common.data;

import com.jnks.iot.server.common.data.id.RuleChainId;

public interface HasRuleEngineProfile {

    RuleChainId getDefaultRuleChainId();

    String getDefaultQueueName();

}
