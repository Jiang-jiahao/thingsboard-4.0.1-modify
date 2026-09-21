package com.jnks.iot.rule.engine.transaction;

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
        type = ComponentType.ACTION,
        name = "同步结束",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "该节点现已弃用。请改用 \"Checkpoint\"。",
        nodeDetails = "",
        configDirective = ("jnksIotNodeEmptyConfig")
)
@Deprecated
public class JnksIotSynchronizationEndNode implements JnksIotNode {

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        log.warn("Synchronization Start/End nodes are deprecated since TB 2.5. Use queue with submit strategy SEQUENTIAL_BY_ORIGINATOR instead.");
        ctx.tellSuccess(msg);
    }

}
