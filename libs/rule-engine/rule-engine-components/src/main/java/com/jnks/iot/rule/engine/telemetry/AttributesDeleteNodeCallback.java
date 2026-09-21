package com.jnks.iot.rule.engine.telemetry;

import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.List;

@Slf4j
public class AttributesDeleteNodeCallback extends TelemetryNodeCallback {

    private String scope;
    private List<String> keys;

    public AttributesDeleteNodeCallback(JnksIotContext ctx, JnksIotMsg msg, String scope, List<String> keys) {
        super(ctx, msg);
        this.scope = scope;
        this.keys = keys;
    }

    @Override
    public void onSuccess(@Nullable Void result) {
        JnksIotContext ctx = this.getCtx();
        JnksIotMsg jnksIotMsg = this.getMsg();
        ctx.enqueue(ctx.attributesDeletedActionMsg(jnksIotMsg.getOriginator(), ctx.getSelfId(), scope, keys),
                () -> ctx.tellSuccess(jnksIotMsg),
                throwable -> ctx.tellFailure(jnksIotMsg, throwable));
    }
}
