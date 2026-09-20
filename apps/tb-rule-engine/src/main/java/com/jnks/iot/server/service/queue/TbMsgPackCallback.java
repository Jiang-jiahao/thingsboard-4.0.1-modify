package com.jnks.iot.server.service.queue;

import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.ExceptionUtil;
import com.jnks.iot.server.common.data.exception.AbstractRateLimitException;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.common.msg.queue.RuleNodeInfo;
import com.jnks.iot.server.common.msg.queue.TbMsgCallback;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TbMsgPackCallback implements TbMsgCallback {
    private final UUID id;
    private final TenantId tenantId;
    private final TbMsgPackProcessingContext ctx;
    private final long startMsgProcessing;
    private final Timer successfulMsgTimer;
    private final Timer failedMsgTimer;

    public TbMsgPackCallback(UUID id, TenantId tenantId, TbMsgPackProcessingContext ctx) {
        this(id, tenantId, ctx, null, null);
    }

    public TbMsgPackCallback(UUID id, TenantId tenantId, TbMsgPackProcessingContext ctx, Timer successfulMsgTimer, Timer failedMsgTimer) {
        this.id = id;
        this.tenantId = tenantId;
        this.ctx = ctx;
        this.successfulMsgTimer = successfulMsgTimer;
        this.failedMsgTimer = failedMsgTimer;
        startMsgProcessing = System.currentTimeMillis();
    }

    @Override
    public void onSuccess() {
        log.trace("[{}] ON SUCCESS", id);
        if (successfulMsgTimer != null) {
            successfulMsgTimer.record(System.currentTimeMillis() - startMsgProcessing, TimeUnit.MILLISECONDS);
        }
        ctx.onSuccess(id);
    }

    @Override
    public void onRateLimit(RuleEngineException e) {
        log.debug("[{}] ON RATE LIMIT", id, e);
        //TODO notify tenant on rate limit
        if (failedMsgTimer != null) {
            failedMsgTimer.record(System.currentTimeMillis() - startMsgProcessing, TimeUnit.MILLISECONDS);
        }
        ctx.onSuccess(id);
    }
    
    @Override
    public void onFailure(RuleEngineException e) {
        if (ExceptionUtil.lookupExceptionInCause(e, AbstractRateLimitException.class) != null) {
            onRateLimit(e);
            return;
        }

        log.trace("[{}] ON FAILURE", id, e);
        if (failedMsgTimer != null) {
            failedMsgTimer.record(System.currentTimeMillis() - startMsgProcessing, TimeUnit.MILLISECONDS);
        }
        ctx.onFailure(tenantId, id, e);
    }

    @Override
    public boolean isMsgValid() {
        return !ctx.isCanceled();
    }

    @Override
    public void onProcessingStart(RuleNodeInfo ruleNodeInfo) {
        log.trace("[{}] ON PROCESSING START: {}", id, ruleNodeInfo);
        ctx.onProcessingStart(id, ruleNodeInfo);
    }

    @Override
    public void onProcessingEnd(RuleNodeId ruleNodeId) {
        log.trace("[{}] ON PROCESSING END: {}", id, ruleNodeId);
        ctx.onProcessingEnd(id, ruleNodeId);
    }
}
