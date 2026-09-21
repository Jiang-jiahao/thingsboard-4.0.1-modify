package com.jnks.iot.server.service.queue;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;

import java.util.UUID;

@Slf4j
public class JnksIotPackCallback<T> implements JnksIotCallback {
    private final JnksIotPackProcessingContext<T> ctx;
    @Getter
    private final UUID id;

    public JnksIotPackCallback(UUID id, JnksIotPackProcessingContext<T> ctx) {
        log.trace("[{}] CALLBACK CREATED", id);
        this.id = id;
        this.ctx = ctx;
    }

    @Override
    public void onSuccess() {
        log.trace("[{}] ON SUCCESS", id);
        ctx.onSuccess(id);
    }

    @Override
    public void onFailure(Throwable t) {
        log.trace("[{}] ON FAILURE", id, t);
        ctx.onFailure(id, t);
    }
}
