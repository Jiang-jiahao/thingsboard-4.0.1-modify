package com.jnks.iot.server.service.queue.processing;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

import java.util.UUID;
import java.util.function.BiConsumer;

@Slf4j
public class BurstJnksIotRuleEngineSubmitStrategy extends AbstractJnksIotRuleEngineSubmitStrategy {

    public BurstJnksIotRuleEngineSubmitStrategy(String queueName) {
        super(queueName);
    }

    @Override
    public void submitAttempt(BiConsumer<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> msgConsumer) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] submitting [{}] messages to rule engine", queueName, orderedMsgList.size());
        }
        orderedMsgList.forEach(pair -> msgConsumer.accept(pair.uuid, pair.msg));
    }

    @Override
    protected void doOnSuccess(UUID id) {

    }
}
