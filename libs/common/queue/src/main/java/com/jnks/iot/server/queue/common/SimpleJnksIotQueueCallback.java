package com.jnks.iot.server.queue.common;

import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsgMetadata;

import java.util.function.Consumer;

public class SimpleJnksIotQueueCallback implements JnksIotQueueCallback {

    private final Consumer<JnksIotQueueMsgMetadata> onSuccess;
    private final Consumer<Throwable> onFailure;

    public SimpleJnksIotQueueCallback(Consumer<JnksIotQueueMsgMetadata> onSuccess, Consumer<Throwable> onFailure) {
        this.onSuccess = onSuccess;
        this.onFailure = onFailure;
    }

    @Override
    public void onSuccess(JnksIotQueueMsgMetadata metadata) {
        if (onSuccess != null) {
            onSuccess.accept(metadata);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        if (onFailure != null) {
            onFailure.accept(t);
        }
    }

}
