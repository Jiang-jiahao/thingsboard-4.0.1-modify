package com.jnks.iot.server.common.data.util;

import com.jnks.iot.server.common.data.exception.JnksIotException;

public interface ThrowingRunnable {

    void run() throws JnksIotException;

    default ThrowingRunnable andThen(ThrowingRunnable after) {
        return () -> {
            this.run();
            after.run();
        };
    }

}
