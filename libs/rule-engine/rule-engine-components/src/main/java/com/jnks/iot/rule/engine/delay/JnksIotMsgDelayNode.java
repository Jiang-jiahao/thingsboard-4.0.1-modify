package com.jnks.iot.rule.engine.delay;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "delay (deprecated)",
        configClazz = JnksIotMsgDelayNodeConfiguration.class,
        nodeDescription = "Delays incoming message (deprecated)",
        nodeDetails = "Delays messages for a configurable period. " +
                "Please note, this node acknowledges the message from the current queue (message will be removed from queue). " +
                "Deprecated because the acknowledged message still stays in memory (to be delayed) and this " +
                "does not guarantee that message will be processed even if the \"retry failures and timeouts\" processing strategy will be chosen.",
        icon = "pause",
        configDirective = "jnksIotActionNodeMsgDelayConfig"
)
public class JnksIotMsgDelayNode implements JnksIotNode {

    private JnksIotMsgDelayNodeConfiguration config;
    private Map<UUID, JnksIotMsg> pendingMsgs;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotMsgDelayNodeConfiguration.class);
        this.pendingMsgs = new HashMap<>();
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        if (msg.isTypeOf(JnksIotMsgType.DELAY_TIMEOUT_SELF_MSG)) {
            JnksIotMsg pendingMsg = pendingMsgs.remove(UUID.fromString(msg.getData()));
            if (pendingMsg != null) {
                ctx.enqueueForTellNext(
                        JnksIotMsg.newMsg()
                                .queueName(pendingMsg.getQueueName())
                                .type(pendingMsg.getType())
                                .originator(pendingMsg.getOriginator())
                                .customerId(pendingMsg.getCustomerId())
                                .copyMetaData(pendingMsg.getMetaData())
                                .data(pendingMsg.getData())
                                .build(),
                        JnksIotNodeConnectionType.SUCCESS
                );
            }
        } else {
            if (pendingMsgs.size() < config.getMaxPendingMsgs()) {
                pendingMsgs.put(msg.getId(), msg);
                JnksIotMsg tickMsg = ctx.newMsg(null, JnksIotMsgType.DELAY_TIMEOUT_SELF_MSG, ctx.getSelfId(), msg.getCustomerId(), JnksIotMsgMetaData.EMPTY, msg.getId().toString());
                ctx.tellSelf(tickMsg, getDelay(msg));
                ctx.ack(msg);
            } else {
                ctx.tellFailure(msg, new RuntimeException("Max limit of pending messages reached!"));
            }
        }
    }

    private long getDelay(JnksIotMsg msg) {
        int periodInSeconds;
        if (config.isUseMetadataPeriodInSecondsPatterns()) {
            if (isParsable(msg, config.getPeriodInSecondsPattern())) {
                periodInSeconds = Integer.parseInt(JnksIotNodeUtils.processPattern(config.getPeriodInSecondsPattern(), msg));
            } else {
                throw new RuntimeException("Can't parse period in seconds from metadata using pattern: " + config.getPeriodInSecondsPattern());
            }
        } else {
            periodInSeconds = config.getPeriodInSeconds();
        }
        return TimeUnit.SECONDS.toMillis(periodInSeconds);
    }

    private boolean isParsable(JnksIotMsg msg, String pattern) {
        return NumberUtils.isParsable(JnksIotNodeUtils.processPattern(pattern, msg));
    }

    @Override
    public void destroy() {
        pendingMsgs.clear();
    }
}
