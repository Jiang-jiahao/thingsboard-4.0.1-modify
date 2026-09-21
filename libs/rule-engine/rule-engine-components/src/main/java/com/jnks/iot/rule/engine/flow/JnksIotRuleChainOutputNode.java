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
        name = "output",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "transfers the message to the caller rule chain",
        nodeDetails = "Produces output of the rule chain processing. " +
                "The output is forwarded to the caller rule chain, as an output of the corresponding \"input\" rule node. " +
                "The output rule node name corresponds to the relation type of the output message, and it is used to forward messages to other rule nodes in the caller rule chain. ",
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
