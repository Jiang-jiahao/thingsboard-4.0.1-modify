package com.jnks.iot.server.actors.calculatedField;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MultipleJnksIotCallback implements JnksIotCallback {
    @Getter
    private final UUID id;
    private final AtomicInteger counter;
    private final JnksIotCallback callback;

    public MultipleJnksIotCallback(int count, JnksIotCallback callback) {
        id = UUID.randomUUID();
        this.counter = new AtomicInteger(count);
        this.callback = callback;
    }

    @Override
    public void onSuccess() {
        onSuccess(1);
    }

    public void onSuccess(int number) {
        log.trace("[{}][{}] onSuccess({})", id, callback.getId(), number);
        if (counter.addAndGet(-number) <= 0) {
            log.trace("[{}][{}] Done.", id, callback.getId());
            callback.onSuccess();
        }
    }

    @Override
    public void onFailure(Throwable t) {
        log.warn("[{}][{}] onFailure.", id, callback.getId());
        callback.onFailure(t);
    }
}
