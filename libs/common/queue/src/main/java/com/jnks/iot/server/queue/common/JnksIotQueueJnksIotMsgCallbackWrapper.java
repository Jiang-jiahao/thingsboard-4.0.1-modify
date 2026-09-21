package com.jnks.iot.server.queue.common;

import com.jnks.iot.server.common.msg.queue.RuleEngineException;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsgMetadata;

public class JnksIotQueueJnksIotMsgCallbackWrapper implements JnksIotQueueCallback {

    private final JnksIotMsgCallback jnksIotMsgCallback;

    public JnksIotQueueJnksIotMsgCallbackWrapper(JnksIotMsgCallback jnksIotMsgCallback) {
        this.jnksIotMsgCallback = jnksIotMsgCallback;
    }

    @Override
    public void onSuccess(JnksIotQueueMsgMetadata metadata) {
        jnksIotMsgCallback.onSuccess();
    }

    @Override
    public void onFailure(Throwable t) {
        jnksIotMsgCallback.onFailure(new RuleEngineException(t.getMessage(), t));
    }
}
