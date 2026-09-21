package com.jnks.iot.server.queue.common;

import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsgMetadata;

import java.util.concurrent.atomic.AtomicInteger;

public class MultipleJnksIotQueueJnksIotMsgCallbackWrapper implements JnksIotQueueCallback {

    private final AtomicInteger jnksIotQueueCallbackCount;
    private final JnksIotMsgCallback jnksIotMsgCallback;

    public MultipleJnksIotQueueJnksIotMsgCallbackWrapper(int jnksIotQueueCallbackCount, JnksIotMsgCallback jnksIotMsgCallback) {
        this.jnksIotQueueCallbackCount = new AtomicInteger(jnksIotQueueCallbackCount);
        this.jnksIotMsgCallback = jnksIotMsgCallback;
    }

    @Override
    public void onSuccess(JnksIotQueueMsgMetadata metadata) {
        if (jnksIotQueueCallbackCount.decrementAndGet() <= 0) {
            jnksIotMsgCallback.onSuccess();
        }
    }

    @Override
    public void onFailure(Throwable t) {
        jnksIotMsgCallback.onFailure(new RuleEngineException(t.getMessage(), t));
    }
}
