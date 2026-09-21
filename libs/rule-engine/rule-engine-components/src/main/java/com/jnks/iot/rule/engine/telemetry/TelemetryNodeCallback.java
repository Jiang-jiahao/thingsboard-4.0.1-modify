package com.jnks.iot.rule.engine.telemetry;

import com.google.common.util.concurrent.FutureCallback;
import jakarta.annotation.Nullable;
import lombok.Data;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.msg.JnksIotMsg;

/**
 * Created by ashvayka on 02.04.18.
 */
@Data
class TelemetryNodeCallback implements FutureCallback<Void> {
    private final JnksIotContext ctx;
    private final JnksIotMsg msg;

    @Override
    public void onSuccess(@Nullable Void result) {
        ctx.tellSuccess(msg);
    }

    @Override
    public void onFailure(Throwable t) {
        ctx.tellFailure(msg, t);
    }
}
