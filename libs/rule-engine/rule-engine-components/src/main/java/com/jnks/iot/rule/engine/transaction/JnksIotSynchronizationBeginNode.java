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
        name = "同步开始",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "此节点已弃用。请改用 \"Checkpoint\"。",
        nodeDetails = "此节点应与 \"synchronization end\" 节点配合使用。 \n 此节点将根据消息来源方 id 将消息放入队列。 \n" +
                "在之前的消息处理完成或发生超时事件之前，后续消息将不会被处理。\n" +
                "每个来源方的队列大小和超时值可在系统级别配置",
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
