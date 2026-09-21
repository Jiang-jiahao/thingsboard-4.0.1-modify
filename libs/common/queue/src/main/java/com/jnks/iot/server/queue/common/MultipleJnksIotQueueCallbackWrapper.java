package com.jnks.iot.server.queue.common;

import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsgMetadata;

import java.util.concurrent.atomic.AtomicInteger;

public class MultipleJnksIotQueueCallbackWrapper implements JnksIotQueueCallback {

    private final AtomicInteger jnksIotQueueCallbackCount;
    private final JnksIotQueueCallback callback;

    public MultipleJnksIotQueueCallbackWrapper(int jnksIotQueueCallbackCount, JnksIotQueueCallback callback) {
        this.jnksIotQueueCallbackCount = new AtomicInteger(jnksIotQueueCallbackCount);
        this.callback = callback;
    }

    @Override
    public void onSuccess(JnksIotQueueMsgMetadata metadata) {
        if (jnksIotQueueCallbackCount.decrementAndGet() <= 0) {
            callback.onSuccess(metadata);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        callback.onFailure(new RuleEngineException(t.getMessage(), t));
    }
}
