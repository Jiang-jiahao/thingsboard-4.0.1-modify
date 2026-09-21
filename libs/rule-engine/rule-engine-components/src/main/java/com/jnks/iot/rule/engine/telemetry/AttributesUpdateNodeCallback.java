package com.jnks.iot.rule.engine.telemetry;

import jakarta.annotation.Nullable;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.List;

public class AttributesUpdateNodeCallback extends TelemetryNodeCallback {

    private final String scope;
    private final List<AttributeKvEntry> attributes;

    public AttributesUpdateNodeCallback(JnksIotContext ctx, JnksIotMsg msg, String scope, List<AttributeKvEntry> attributes) {
        super(ctx, msg);
        this.scope = scope;
        this.attributes = attributes;
    }

    @Override
    public void onSuccess(@Nullable Void result) {
        JnksIotContext ctx = this.getCtx();
        JnksIotMsg jnksIotMsg = this.getMsg();
        ctx.enqueue(ctx.attributesUpdatedActionMsg(jnksIotMsg.getOriginator(), ctx.getSelfId(), scope, attributes),
                () -> ctx.tellSuccess(jnksIotMsg),
                throwable -> ctx.tellFailure(jnksIotMsg, throwable));
    }
}
