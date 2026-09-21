package com.jnks.iot.rule.engine.filter;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "消息类型切换",
        configClazz = EmptyNodeConfiguration.class,
        relationTypes = {}, // should always be empty. We add the relation types for this node in AnnotationComponentDiscoveryService.
        nodeDescription = "Route incoming messages by Message Type",
        nodeDetails = "通过相应的链发送消息类型为 <b>\"Post attributes\"、\"Post telemetry\"、\"RPC Request\"</b>" +
                " 等的消息，否则使用 <b>Other</b> 链。<br><br>" +
                "输出连接：<i>消息类型连接</i>、<code>Other</code>（若消息类型为自定义）或 <code>Failure</code>",
        configDirective = "jnksIotNodeEmptyConfig")
public class JnksIotMsgTypeSwitchNode implements JnksIotNode {

    EmptyNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        ctx.tellNext(msg, msg.getInternalType().getRuleNodeConnection());
    }

}
