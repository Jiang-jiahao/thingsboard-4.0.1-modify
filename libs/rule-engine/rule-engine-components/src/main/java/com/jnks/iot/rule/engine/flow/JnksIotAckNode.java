package com.jnks.iot.rule.engine.flow;

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
        type = ComponentType.FLOW,
        name = "确认",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "确认传入消息",
        nodeDetails = "确认后，消息会被推送到相关的规则节点。如果不关心该消息后续如何处理，可使用此节点。",
        configDirective = "jnksIotNodeEmptyConfig"
)
public class JnksIotAckNode implements JnksIotNode {

    EmptyNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        ctx.ack(msg);
        ctx.tellSuccess(msg);
    }

}
