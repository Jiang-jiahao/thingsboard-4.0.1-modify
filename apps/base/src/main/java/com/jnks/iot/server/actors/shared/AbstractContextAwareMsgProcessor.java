package com.jnks.iot.server.actors.shared;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

@Slf4j
public abstract class AbstractContextAwareMsgProcessor {

    protected final ActorSystemContext systemContext;

    protected AbstractContextAwareMsgProcessor(ActorSystemContext systemContext) {
        super();
        this.systemContext = systemContext;
    }

    private ScheduledExecutorService getScheduler() {
        return systemContext.getScheduler();
    }

    protected ScheduledFuture<?> schedulePeriodicMsgWithDelay(JnksIotActorCtx ctx, JnksIotActorMsg msg, long delayInMs, long periodInMs) {
        return systemContext.schedulePeriodicMsgWithDelay(ctx, msg, delayInMs, periodInMs);
    }

    protected void scheduleMsgWithDelay(JnksIotActorCtx ctx, JnksIotActorMsg msg, long delayInMs) {
        systemContext.scheduleMsgWithDelay(ctx, msg, delayInMs);
    }

}
