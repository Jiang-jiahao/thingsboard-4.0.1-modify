package com.jnks.iot.rule.engine.rest;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.UUID;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "rest call reply",
        configClazz = JnksIotSendRestApiCallReplyNodeConfiguration.class,
        nodeDescription = "Sends reply to REST API call to rule engine",
        nodeDetails = "Expects messages with any message type. Forwards incoming message as a reply to REST API call sent to rule engine.",
        configDirective = "jnksIotActionNodeSendRestApiCallReplyConfig",
        icon = "call_merge"
)
public class JnksIotSendRestApiCallReplyNode implements JnksIotNode {

    private JnksIotSendRestApiCallReplyNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotSendRestApiCallReplyNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        String serviceIdStr = msg.getMetaData().getValue(config.getServiceIdMetaDataAttribute());
        String requestIdStr = msg.getMetaData().getValue(config.getRequestIdMetaDataAttribute());
        if (StringUtils.isEmpty(requestIdStr)) {
            ctx.tellFailure(msg, new RuntimeException("Request id is not present in the metadata!"));
        } else if (StringUtils.isEmpty(serviceIdStr)) {
            ctx.tellFailure(msg, new RuntimeException("Service id is not present in the metadata!"));
        } else if (StringUtils.isEmpty(msg.getData())) {
            ctx.tellFailure(msg, new RuntimeException("Request body is empty!"));
        } else {
            ctx.getRpcService().sendRestApiCallReply(serviceIdStr, UUID.fromString(requestIdStr), msg);
            ctx.tellSuccess(msg);
        }
    }
}
