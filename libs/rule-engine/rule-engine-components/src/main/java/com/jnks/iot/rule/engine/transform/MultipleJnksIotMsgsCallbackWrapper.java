package com.jnks.iot.rule.engine.transform;

import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;

import java.util.concurrent.atomic.AtomicInteger;

public class MultipleJnksIotMsgsCallbackWrapper implements JnksIotMsgCallbackWrapper {

    private final AtomicInteger jnksIotMsgsCallbackCount;
    private final JnksIotMsgCallback callback;

    public MultipleJnksIotMsgsCallbackWrapper(int jnksIotMsgsCallbackCount, JnksIotMsgCallback callback) {
        this.jnksIotMsgsCallbackCount = new AtomicInteger(jnksIotMsgsCallbackCount);
        this.callback = callback;
    }

    @Override
    public void onSuccess() {
        if (jnksIotMsgsCallbackCount.decrementAndGet() <= 0) {
            callback.onSuccess();
        }
    }

    @Override
    public void onFailure(Throwable t) {
        callback.onFailure(new RuleEngineException(t.getMessage(), t));
    }
}

