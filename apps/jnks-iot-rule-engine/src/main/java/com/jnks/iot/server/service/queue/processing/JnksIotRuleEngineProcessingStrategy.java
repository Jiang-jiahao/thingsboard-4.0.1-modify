package com.jnks.iot.server.service.queue.processing;

public interface JnksIotRuleEngineProcessingStrategy {

    boolean isSkipTimeoutMsgs();

    JnksIotRuleEngineProcessingDecision analyze(JnksIotRuleEngineProcessingResult result);

}
