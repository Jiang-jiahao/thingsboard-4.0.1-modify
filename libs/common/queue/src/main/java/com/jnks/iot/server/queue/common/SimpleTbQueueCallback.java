package com.jnks.iot.server.queue.common;

import com.jnks.iot.server.queue.TbQueueCallback;
import com.jnks.iot.server.queue.TbQueueMsgMetadata;

import java.util.function.Consumer;

public class SimpleTbQueueCallback implements TbQueueCallback {

    private final Consumer<TbQueueMsgMetadata> onSuccess;
    private final Consumer<Throwable> onFailure;

    public SimpleTbQueueCallback(Consumer<TbQueueMsgMetadata> onSuccess, Consumer<Throwable> onFailure) {
        this.onSuccess = onSuccess;
        this.onFailure = onFailure;
    }

    @Override
    public void onSuccess(TbQueueMsgMetadata metadata) {
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
