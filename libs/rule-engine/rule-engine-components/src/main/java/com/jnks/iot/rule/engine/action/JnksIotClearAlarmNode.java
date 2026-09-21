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
        name = "清除告警", relationTypes = {"Cleared", "False"},
        configClazz = JnksIotClearAlarmNodeConfiguration.class,
        nodeDescription = "清除告警",
        nodeDetails =
                "详情 - 基于传入消息创建 JSON 对象的 JS 函数。该对象将被添加到 Alarm.details 字段中。\n" +
                        "节点输出：\n" +
                        "如果告警未被清除，则返回原始消息。否则返回新的 Message，其类型为 'ALARM'，'msg' 属性中包含 Alarm 对象，'metadata' 中将包含 'isClearedAlarm' 属性。 " +
                        "消息负载可通过 <code>msg</code> 属性访问。例如 <code>'temperature = ' + msg.temperature ;</code>。 " +
                        "消息元数据可通过 <code>metadata</code> 属性访问。例如 <code>'name = ' + metadata.customerName;</code>。",
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
