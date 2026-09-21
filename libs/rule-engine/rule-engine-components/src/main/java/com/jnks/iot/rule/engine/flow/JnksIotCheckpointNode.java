package com.jnks.iot.rule.engine.flow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import static com.jnks.iot.server.common.data.DataConstants.QUEUE_NAME;

@Slf4j
@RuleNode(
        type = ComponentType.FLOW,
        name = "checkpoint",
        configClazz = EmptyNodeConfiguration.class,
        version = 1,
        hasQueueName = true,
        nodeDescription = "transfers the message to another queue",
        nodeDetails = "After successful transfer incoming message is automatically acknowledged. Queue name is configurable.",
        configDirective = "jnksIotNodeEmptyConfig"
)
public class JnksIotCheckpointNode implements JnksIotNode {

    private String queueName;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.queueName = ctx.getQueueName();
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        ctx.enqueueForTellNext(msg, queueName, JnksIotNodeConnectionType.SUCCESS, () -> ctx.ack(msg), error -> ctx.tellFailure(msg, error));
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0:
                if (oldConfiguration.has(QUEUE_NAME)) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).remove(QUEUE_NAME);
                }
                break;
            default:
                break;
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

}
