package com.jnks.iot.rule.engine.action;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.EnumUtils;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmApiCallResult;
import com.jnks.iot.server.common.data.alarm.AlarmCreateOrUpdateActiveRequest;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.alarm.AlarmUpdateRequest;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.io.IOException;
import java.util.List;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "创建告警", relationTypes = {"Created", "Updated", "False"},
        configClazz = JnksIotCreateAlarmNodeConfiguration.class,
        nodeDescription = "创建或更新告警",
        nodeDetails =
                "Details - 用于根据传入消息创建 JSON 对象的 JS 函数。该对象将加入 Alarm.details 字段。\n" +
                        "节点输出：\n" +
                        "若未创建告警，则返回原始消息。否则返回新的 Message，类型为 'ALARM'，'msg' 属性中包含 Alarm 对象，且 'metadata' 将包含 'isNewAlarm/isExistingAlarm' 其中之一。 " +
                        "可通过 <code>msg</code> 属性访问消息负载。例如 <code>'temperature = ' + msg.temperature ;</code>。 " +
                        "可通过 <code>metadata</code> 属性访问消息元数据。例如 <code>'name = ' + metadata.customerName;</code>。",
        configDirective = "jnksIotActionNodeCreateAlarmConfig",
        icon = "notifications_active"
)
public class JnksIotCreateAlarmNode extends JnksIotAbstractAlarmNode<JnksIotCreateAlarmNodeConfiguration> {

    private List<String> relationTypes;
    private AlarmSeverity notDynamicAlarmSeverity;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx, configuration);
        if (!this.config.isDynamicSeverity()) {
            this.notDynamicAlarmSeverity = EnumUtils.getEnum(AlarmSeverity.class, this.config.getSeverity());
            if (this.notDynamicAlarmSeverity == null) {
                throw new JnksIotNodeException("Incorrect Alarm Severity value: " + this.config.getSeverity(), true);
            }
        }
    }


    @Override
    protected JnksIotCreateAlarmNodeConfiguration loadAlarmNodeConfig(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        JnksIotCreateAlarmNodeConfiguration nodeConfiguration = JnksIotNodeUtils.convert(configuration, JnksIotCreateAlarmNodeConfiguration.class);
        relationTypes = nodeConfiguration.getRelationTypes();
        return nodeConfiguration;
    }

    @Override
    protected ListenableFuture<JnksIotAlarmResult> processAlarm(JnksIotContext ctx, JnksIotMsg msg) {
        String alarmType;
        final Alarm msgAlarm;

        if (!config.isUseMessageAlarmData()) {
            alarmType = JnksIotNodeUtils.processPattern(this.config.getAlarmType(), msg);
            msgAlarm = null;
        } else {
            try {
                msgAlarm = getAlarmFromMessage(ctx, msg);
                alarmType = msgAlarm.getType();
            } catch (IOException e) {
                ctx.tellFailure(msg, e);
                return null;
            }
        }

        Alarm existingAlarm = ctx.getAlarmService().findLatestActiveByOriginatorAndType(ctx.getTenantId(), msg.getOriginator(), alarmType);
        if (existingAlarm == null || existingAlarm.getStatus().isCleared()) {
            return createNewAlarm(ctx, msg, msgAlarm);
        } else {
            return updateAlarm(ctx, msg, existingAlarm, msgAlarm);
        }
    }

    private Alarm getAlarmFromMessage(JnksIotContext ctx, JnksIotMsg msg) throws IOException {
        Alarm msgAlarm;
        msgAlarm = JacksonUtil.fromString(msg.getData(), Alarm.class);
        msgAlarm.setTenantId(ctx.getTenantId());
        if (msgAlarm.getOriginator() == null) {
            msgAlarm.setOriginator(msg.getOriginator());
        }
        return msgAlarm;
    }

    private ListenableFuture<JnksIotAlarmResult> createNewAlarm(JnksIotContext ctx, JnksIotMsg msg, Alarm msgAlarm) {
        ListenableFuture<JsonNode> asyncDetails;
        boolean buildDetails = !config.isUseMessageAlarmData() || config.isOverwriteAlarmDetails();
        if (buildDetails) {
            asyncDetails = buildAlarmDetails(msg, null);
        } else {
            asyncDetails = Futures.immediateFuture(null);
        }
        ListenableFuture<Alarm> asyncAlarm = Futures.transform(asyncDetails, details -> {
            Alarm newAlarm;
            if (msgAlarm != null) {
                newAlarm = msgAlarm;
                if (buildDetails) {
                    newAlarm.setDetails(details);
                }
            } else {
                newAlarm = buildAlarm(msg, details, ctx.getTenantId());
            }
            return newAlarm;
        }, MoreExecutors.directExecutor());
        ListenableFuture<AlarmApiCallResult> asyncCreated = Futures.transform(asyncAlarm,
                alarm -> ctx.getAlarmService().createAlarm(AlarmCreateOrUpdateActiveRequest.fromAlarm(alarm)), ctx.getDbCallbackExecutor());
        return Futures.transform(asyncCreated, JnksIotAlarmResult::fromAlarmResult, MoreExecutors.directExecutor());
    }

    private ListenableFuture<JnksIotAlarmResult> updateAlarm(JnksIotContext ctx, JnksIotMsg msg, Alarm existingAlarm, Alarm msgAlarm) {
        ListenableFuture<JsonNode> asyncDetails;
        boolean buildDetails = !config.isUseMessageAlarmData() || config.isOverwriteAlarmDetails();
        if (buildDetails) {
            asyncDetails = buildAlarmDetails(msg, existingAlarm.getDetails());
        } else {
            asyncDetails = Futures.immediateFuture(null);
        }
        ListenableFuture<AlarmApiCallResult> asyncUpdated = Futures.transform(asyncDetails, details -> {
            if (msgAlarm != null) {
                existingAlarm.setSeverity(msgAlarm.getSeverity());
                existingAlarm.setPropagate(msgAlarm.isPropagate());
                existingAlarm.setPropagateToOwner(msgAlarm.isPropagateToOwner());
                existingAlarm.setPropagateToTenant(msgAlarm.isPropagateToTenant());
                existingAlarm.setPropagateRelationTypes(msgAlarm.getPropagateRelationTypes());
                if (buildDetails) {
                    existingAlarm.setDetails(details);
                } else {
                    existingAlarm.setDetails(msgAlarm.getDetails());
                }
            } else {
                existingAlarm.setSeverity(processAlarmSeverity(msg));
                existingAlarm.setPropagate(config.isPropagate());
                existingAlarm.setPropagateToOwner(config.isPropagateToOwner());
                existingAlarm.setPropagateToTenant(config.isPropagateToTenant());
                existingAlarm.setPropagateRelationTypes(relationTypes);
                existingAlarm.setDetails(details);
            }
            existingAlarm.setEndTs(currentTimeMillis());
            return ctx.getAlarmService().updateAlarm(AlarmUpdateRequest.fromAlarm(existingAlarm));
        }, ctx.getDbCallbackExecutor());
        return Futures.transform(asyncUpdated, JnksIotAlarmResult::fromAlarmResult, MoreExecutors.directExecutor());
    }

    private Alarm buildAlarm(JnksIotMsg msg, JsonNode details, TenantId tenantId) {
        long ts = msg.getMetaDataTs();
        return Alarm.builder()
                .tenantId(tenantId)
                .originator(msg.getOriginator())
                .cleared(false)
                .acknowledged(false)
                .severity(this.config.isDynamicSeverity() ? processAlarmSeverity(msg) : notDynamicAlarmSeverity)
                .propagate(config.isPropagate())
                .propagateToOwner(config.isPropagateToOwner())
                .propagateToTenant(config.isPropagateToTenant())
                .type(JnksIotNodeUtils.processPattern(this.config.getAlarmType(), msg))
                .propagateRelationTypes(relationTypes)
                .startTs(ts)
                .endTs(ts)
                .details(details)
                .build();
    }

    private AlarmSeverity processAlarmSeverity(JnksIotMsg msg) {
        AlarmSeverity severity = EnumUtils.getEnum(AlarmSeverity.class, JnksIotNodeUtils.processPattern(this.config.getSeverity(), msg));
        if (severity == null) {
            throw new RuntimeException("Used incorrect pattern or Alarm Severity not included in message");
        }
        return severity;
    }

    long currentTimeMillis() {
        return System.currentTimeMillis();
    }

}
