package com.jnks.iot.rule.engine.notification;

import com.jnks.iot.common.util.DonAsynchron;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.external.JnksIotAbstractExternalNode;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.concurrent.ExecutionException;

@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "send to slack",
        configClazz = JnksIotSlackNodeConfiguration.class,
        nodeDescription = "Send message via Slack",
        nodeDetails = "Sends message to a Slack channel or user",
        configDirective = "jnksIotExternalNodeSlackConfig",
        iconUrl = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZD0iTTYsMTVBMiwyIDAgMCwxIDQsMTdBMiwyIDAgMCwxIDIsMTVBMiwyIDAgMCwxIDQsMTNINlYxNU03LDE1QTIsMiAwIDAsMSA5LDEzQTIsMiAwIDAsMSAxMSwxNVYyMEEyLDIgMCAwLDEgOSwyMkEyLDIgMCAwLDEgNywyMFYxNU05LDdBMiwyIDAgMCwxIDcsNUEyLDIgMCAwLDEgOSwzQTIsMiAwIDAsMSAxMSw1VjdIOU05LDhBMiwyIDAgMCwxIDExLDEwQTIsMiAwIDAsMSA5LDEySDRBMiwyIDAgMCwxIDIsMTBBMiwyIDAgMCwxIDQsOEg5TTE3LDEwQTIsMiAwIDAsMSAxOSw4QTIsMiAwIDAsMSAyMSwxMEEyLDIgMCAwLDEgMTksMTJIMTdWMTBNMTYsMTBBMiwyIDAgMCwxIDE0LDEyQTIsMiAwIDAsMSAxMiwxMFY1QTIsMiAwIDAsMSAxNCwzQTIsMiAwIDAsMSAxNiw1VjEwTTE0LDE4QTIsMiAwIDAsMSAxNiwyMEEyLDIgMCAwLDEgMTQsMjJBMiwyIDAgMCwxIDEyLDIwVjE4SDE0TTE0LDE3QTIsMiAwIDAsMSAxMiwxNUEyLDIgMCAwLDEgMTQsMTNIMTlBMiwyIDAgMCwxIDIxLDE1QTIsMiAwIDAsMSAxOSwxN0gxNFoiIC8+PC9zdmc+"
)
public class JnksIotSlackNode extends JnksIotAbstractExternalNode {

    private JnksIotSlackNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx);
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotSlackNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        String token;
        if (config.isUseSystemSettings()) {
            token = ctx.getSlackService().getToken(ctx.getTenantId());
        } else {
            token = config.getBotToken();
        }
        if (token == null) {
            throw new IllegalArgumentException("Slack token is missing");
        }

        String message = JnksIotNodeUtils.processPattern(config.getMessageTemplate(), msg);
        var jnksIotMsg = ackIfNeeded(ctx, msg);
        DonAsynchron.withCallback(ctx.getExternalCallExecutor().executeAsync(() -> {
                    ctx.getSlackService().sendMessage(ctx.getTenantId(), token, config.getConversation().getId(), message);
                }),
                r -> tellSuccess(ctx, jnksIotMsg),
                e -> tellFailure(ctx, jnksIotMsg, e));
    }

}
