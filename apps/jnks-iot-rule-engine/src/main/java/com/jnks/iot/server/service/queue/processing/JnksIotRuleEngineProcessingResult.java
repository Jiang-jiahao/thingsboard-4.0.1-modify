package com.jnks.iot.server.service.queue.processing;

import lombok.Getter;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.service.queue.JnksIotMsgPackProcessingContext;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class JnksIotRuleEngineProcessingResult {

    @Getter
    private final String queueName;
    @Getter
    private final boolean success;
    @Getter
    private final boolean timeout;
    @Getter
    private final JnksIotMsgPackProcessingContext ctx;

    public JnksIotRuleEngineProcessingResult(String queueName, boolean timeout, JnksIotMsgPackProcessingContext ctx) {
        this.queueName = queueName;
        this.timeout = timeout;
        this.ctx = ctx;
        this.success = !timeout && ctx.getPendingMap().isEmpty() && ctx.getFailedMap().isEmpty();
    }

    public ConcurrentMap<UUID, JnksIotProtoQueueMsg<ToRuleEngineMsg>> getPendingMap() {
        return ctx.getPendingMap();
    }

    public ConcurrentMap<UUID, JnksIotProtoQueueMsg<ToRuleEngineMsg>> getSuccessMap() {
        return ctx.getSuccessMap();
    }

    public ConcurrentMap<UUID, JnksIotProtoQueueMsg<ToRuleEngineMsg>> getFailedMap() {
        return ctx.getFailedMap();
    }

    public ConcurrentMap<TenantId, RuleEngineException> getExceptionsMap() {
        return ctx.getExceptionsMap();
    }
}
