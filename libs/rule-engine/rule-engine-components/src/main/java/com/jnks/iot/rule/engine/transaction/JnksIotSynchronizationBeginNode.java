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
        name = "synchronization start",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "This Node is now deprecated. Use \"Checkpoint\" instead.",
        nodeDetails = "This node should be used together with \"synchronization end\" node. \n This node will put messages into queue based on message originator id. \n" +
                "Subsequent messages will not be processed until the previous message processing is completed or timeout event occurs.\n" +
                "Size of the queue per originator and timeout values are configurable on a system level",
        configDirective = "jnksIotNodeEmptyConfig")
@Deprecated
public class JnksIotSynchronizationBeginNode implements JnksIotNode {

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        log.warn("Synchronization Start/End nodes are deprecated since TB 2.5. Use queue with submit strategy SEQUENTIAL_BY_ORIGINATOR instead.");
        ctx.tellSuccess(msg);
    }

}
