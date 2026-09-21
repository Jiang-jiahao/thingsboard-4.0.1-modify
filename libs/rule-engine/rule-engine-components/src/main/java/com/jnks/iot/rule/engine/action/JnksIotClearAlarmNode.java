package com.jnks.iot.rule.engine.action;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmApiCallResult;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "clear alarm", relationTypes = {"Cleared", "False"},
        configClazz = JnksIotClearAlarmNodeConfiguration.class,
        nodeDescription = "Clear Alarm",
        nodeDetails =
                "Details - JS function that creates JSON object based on incoming message. This object will be added into Alarm.details field.\n" +
                        "Node output:\n" +
                        "If alarm was not cleared, original message is returned. Otherwise new Message returned with type 'ALARM', Alarm object in 'msg' property and 'metadata' will contains 'isClearedAlarm' property. " +
                        "Message payload can be accessed via <code>msg</code> property. For example <code>'temperature = ' + msg.temperature ;</code>. " +
                        "Message metadata can be accessed via <code>metadata</code> property. For example <code>'name = ' + metadata.customerName;</code>.",
        configDirective = "jnksIotActionNodeClearAlarmConfig",
        icon = "notifications_off"
)
public class JnksIotClearAlarmNode extends JnksIotAbstractAlarmNode<JnksIotClearAlarmNodeConfiguration> {

    @Override
    protected JnksIotClearAlarmNodeConfiguration loadAlarmNodeConfig(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        return JnksIotNodeUtils.convert(configuration, JnksIotClearAlarmNodeConfiguration.class);
    }

    @Override
    protected ListenableFuture<JnksIotAlarmResult> processAlarm(JnksIotContext ctx, JnksIotMsg msg) {
        String alarmType = JnksIotNodeUtils.processPattern(this.config.getAlarmType(), msg);
        Alarm alarm;
        if (msg.getOriginator().getEntityType().equals(EntityType.ALARM)) {
            alarm = ctx.getAlarmService().findAlarmById(ctx.getTenantId(), new AlarmId(msg.getOriginator().getId()));
        } else {
            alarm = ctx.getAlarmService().findLatestActiveByOriginatorAndType(ctx.getTenantId(), msg.getOriginator(), alarmType);
        }
        if (alarm != null && !alarm.getStatus().isCleared()) {
            return clearAlarm(ctx, msg, alarm);
        }
        return Futures.immediateFuture(new JnksIotAlarmResult(false, false, false, null));
    }

    private ListenableFuture<JnksIotAlarmResult> clearAlarm(JnksIotContext ctx, JnksIotMsg msg, Alarm alarm) {
        ListenableFuture<JsonNode> asyncDetails = buildAlarmDetails(msg, alarm.getDetails());
        return Futures.transform(asyncDetails, details -> {
            AlarmApiCallResult result = ctx.getAlarmService().clearAlarm(ctx.getTenantId(), alarm.getId(), System.currentTimeMillis(), details);
            if (result.isSuccessful()) {
                return new JnksIotAlarmResult(false, false, result.isCleared(), result.getAlarm());
            } else {
                return new JnksIotAlarmResult(false, false, false, alarm);
            }
        }, ctx.getDbCallbackExecutor());
    }
}
