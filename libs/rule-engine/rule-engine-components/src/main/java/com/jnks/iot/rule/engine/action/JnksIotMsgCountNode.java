package com.jnks.iot.rule.engine.action;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "消息计数",
        configClazz = JnksIotMsgCountNodeConfiguration.class,
        nodeDescription = "统计收到的消息数量",
        nodeDetails = "按指定间隔统计收到的消息数量，并生成带有消息计数的 POST_TELEMETRY_REQUEST 消息",
        icon = "functions",
        configDirective = "jnksIotActionNodeMsgCountConfig"
)
public class JnksIotMsgCountNode implements JnksIotNode {

    private AtomicLong messagesProcessed = new AtomicLong(0);
    private final Gson gson = new Gson();
    private UUID nextTickId;
    private long delay;
    private String telemetryPrefix;
    private long lastScheduledTs;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        JnksIotMsgCountNodeConfiguration config = JnksIotNodeUtils.convert(configuration, JnksIotMsgCountNodeConfiguration.class);
        this.delay = TimeUnit.SECONDS.toMillis(config.getInterval());
        this.telemetryPrefix = config.getTelemetryPrefix();
        scheduleTickMsg(ctx, null);

    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        if (msg.isTypeOf(JnksIotMsgType.MSG_COUNT_SELF_MSG) && msg.getId().equals(nextTickId)) {
            JsonObject telemetryJson = new JsonObject();
            telemetryJson.addProperty(this.telemetryPrefix + "_" + ctx.getServiceId(), messagesProcessed.longValue());

            messagesProcessed = new AtomicLong(0);

            JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
            metaData.putValue("delta", Long.toString(System.currentTimeMillis() - lastScheduledTs + delay));

            JnksIotMsg jnksIotMsg = JnksIotMsg.newMsg()
                    .queueName(msg.getQueueName())
                    .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                    .originator(ctx.getTenantId())
                    .customerId(msg.getCustomerId())
                    .copyMetaData(metaData)
                    .data(gson.toJson(telemetryJson))
                    .build();
            ctx.enqueueForTellNext(jnksIotMsg, JnksIotNodeConnectionType.SUCCESS);
            scheduleTickMsg(ctx, jnksIotMsg);
        } else {
            messagesProcessed.incrementAndGet();
            ctx.ack(msg);
        }
    }

    private void scheduleTickMsg(JnksIotContext ctx, JnksIotMsg msg) {
        long curTs = System.currentTimeMillis();
        if (lastScheduledTs == 0L) {
            lastScheduledTs = curTs;
        }
        lastScheduledTs = lastScheduledTs + delay;
        long curDelay = Math.max(0L, (lastScheduledTs - curTs));
        JnksIotMsg tickMsg = ctx.newMsg(null, JnksIotMsgType.MSG_COUNT_SELF_MSG, ctx.getSelfId(), msg != null ? msg.getCustomerId() : null, JnksIotMsgMetaData.EMPTY, JnksIotMsg.EMPTY_STRING);
        nextTickId = tickMsg.getId();
        ctx.tellSelf(tickMsg, curDelay);
    }

}
