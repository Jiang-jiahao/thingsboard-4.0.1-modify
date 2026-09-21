package com.jnks.iot.rule.engine.flow;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

@Slf4j
@RuleNode(
        type = ComponentType.FLOW,
        name = "输出",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "将消息转移到调用方规则链",
        nodeDetails = "产生规则链处理的输出。 " +
                "该输出将转发到调用方规则链，作为相应 \"input\" 规则节点的输出。 " +
                "输出规则节点的名称对应于输出消息的关系类型，用于将消息转发到调用方规则链中的其他规则节点。 ",
        configDirective = "jnksIotFlowNodeRuleChainOutputConfig",
        outEnabled = false
)
public class JnksIotRuleChainOutputNode implements JnksIotNode {

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        ctx.output(msg, ctx.getSelf().getName());
    }

}
