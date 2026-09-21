package com.jnks.iot.server.actors.service;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.actors.AbstractJnksIotActor;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.ProcessFailureStrategy;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

@Slf4j
public abstract class ContextAwareActor extends AbstractJnksIotActor {

    public static final int ENTITY_PACK_LIMIT = 1024;

    protected final ActorSystemContext systemContext;

    public ContextAwareActor(ActorSystemContext systemContext) {
        super();
        this.systemContext = systemContext;
    }

    @Override
    public boolean process(JnksIotActorMsg msg) {
        if (log.isDebugEnabled()) {
            log.debug("Processing msg: {}", msg);
        }
        if (!doProcess(msg)) {
            log.warn("Unprocessed message: {}!", msg);
        }
        return false;
    }

    protected abstract boolean doProcess(JnksIotActorMsg msg);

    @Override
    public ProcessFailureStrategy onProcessFailure(JnksIotActorMsg msg, Throwable t) {
        log.debug("[{}] Processing failure for msg {}", getActorRef().getActorId(), msg, t);
        return doProcessFailure(t);
    }

    protected ProcessFailureStrategy doProcessFailure(Throwable t) {
        if (t instanceof Error) {
            return ProcessFailureStrategy.stop();
        } else {
            return ProcessFailureStrategy.resume();
        }
    }
}
