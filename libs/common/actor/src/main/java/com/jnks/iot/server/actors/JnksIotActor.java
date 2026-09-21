package com.jnks.iot.server.actors;

import com.jnks.iot.server.common.msg.JnksIotActorMsg;
import com.jnks.iot.server.common.msg.JnksIotActorStopReason;

public interface JnksIotActor {

    boolean process(JnksIotActorMsg msg);

    JnksIotActorRef getActorRef();

    default void init(JnksIotActorCtx ctx) throws JnksIotActorException {
    }

    default void destroy(JnksIotActorStopReason stopReason, Throwable cause) throws JnksIotActorException {
    }

    default InitFailureStrategy onInitFailure(int attempt, Throwable t) {
        return InitFailureStrategy.retryWithDelay(5000L * attempt);
    }

    default ProcessFailureStrategy onProcessFailure(JnksIotActorMsg msg, Throwable t) {
        if (t instanceof Error) {
            return ProcessFailureStrategy.stop();
        } else {
            return ProcessFailureStrategy.resume();
        }
    }
}
