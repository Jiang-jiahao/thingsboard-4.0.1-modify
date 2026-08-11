/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.action;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.cluster.TbClusterService;
import org.thingsboard.server.common.data.*;
import org.thingsboard.server.common.data.alarm.Alarm;
import org.thingsboard.server.common.data.alarm.AlarmComment;
import org.thingsboard.server.common.data.alarm.AlarmInfo;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.HasId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.msg.TbMsgType;
import org.thingsboard.server.common.data.notification.rule.trigger.AlarmAssignmentTrigger;
import org.thingsboard.server.common.data.notification.rule.trigger.AlarmCommentTrigger;
import org.thingsboard.server.common.data.notification.rule.trigger.EntitiesLimitTrigger;
import org.thingsboard.server.common.data.notification.rule.trigger.EntityActionTrigger;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgDataType;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.msg.notification.NotificationRuleProcessor;
import org.thingsboard.server.dao.audit.AuditLogService;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 实体动作的落库与规则引擎推送实现。
 * <p>
 * 由 {@link org.thingsboard.server.service.entitiy.DefaultTbLogEntityActionService} 调用，负责两件事：
 * <ul>
 *   <li>{@link #logEntityAction}：有用户上下文时写审计日志；成功时还会推规则引擎；</li>
 *   <li>{@link #pushEntityActionToRuleEngine}：把动作组装为 {@link TbMsg} 推到 Rule Engine，
 *       并按动作类型触发通知规则（实体限额、实体动作、告警分配、告警评论等）。</li>
 * </ul>
 * {@code additionalInfo} 按 {@link ActionType} 约定下标取值（客户/租户/Edge 分配、属性与时序变更、关系对象等），
 * 通过 {@link #extractParameter} 安全提取；无法映射到规则引擎消息类型的动作会直接跳过推送。
 *
 * @see org.thingsboard.server.service.entitiy.TbLogEntityActionService
 * @see AuditLogService
 * @see NotificationRuleProcessor
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EntityActionService {

    /** 将 TbMsg 推入规则引擎（按租户与实体分区路由） */
    private final TbClusterService tbClusterService;
    /** 持久化审计日志 */
    private final AuditLogService auditLogService;
    /** 处理通知规则触发器 */
    private final NotificationRuleProcessor notificationRuleProcessor;

    /**
     * 将实体动作推送到规则引擎，并在适用时触发通知规则。
     * <p>
     * 仅当 {@link ActionType#getRuleEngineMsgType()} 有对应 {@link TbMsgType} 时执行。
     * 处理步骤概要：
     * <ol>
     *   <li>组装元数据（用户、客户、各类 ASSIGNED_* / 评论等）；</li>
     *   <li>将实体序列化为 JSON 节点；无实体快照时按动作类型从 {@code additionalInfo}
     *       填充属性、时序或关系内容（仪表板会清空 configuration 字段以减小消息体）；</li>
     *   <li>补全 tenantId（可从实现了 {@link HasTenantId} 的实体读取）；</li>
     *   <li>非系统租户时调用 {@link #processNotificationRules}；</li>
     *   <li>构造 {@link TbMsg} 并 {@link TbClusterService#pushMsgToRuleEngine}。</li>
     * </ol>
     * 组装或推送失败只记警告日志，不向外抛出，避免拖垮上层业务事务。
     *
     * @param entityId       动作主体实体 ID（亦作 TbMsg originator）
     * @param entity         实体快照；可为 {@code null}（属性/时序/关系等场景用 additionalInfo）
     * @param tenantId       租户；可为 {@code null}，随后尝试从实体补全
     * @param customerId     客户；空或 nullUid 时不写入元数据
     * @param actionType     动作类型
     * @param user           操作用户；系统调用可为 {@code null}
     * @param additionalInfo 随动作类型变化的附加参数
     */
    public void pushEntityActionToRuleEngine(EntityId entityId, HasName entity, TenantId tenantId, CustomerId customerId,
                                             ActionType actionType, User user, Object... additionalInfo) {
        Optional<TbMsgType> msgType = actionType.getRuleEngineMsgType();
        if (msgType.isPresent()) {
            try {
                TbMsgMetaData metaData = new TbMsgMetaData();
                if (user != null) {
                    metaData.putValue("userId", user.getId().toString());
                    metaData.putValue("userName", user.getName());
                    metaData.putValue("userEmail", user.getEmail());
                    if (user.getFirstName() != null) {
                        metaData.putValue("userFirstName", user.getFirstName());
                    }
                    if (user.getLastName() != null) {
                        metaData.putValue("userLastName", user.getLastName());
                    }
                }
                if (customerId != null && !customerId.isNullUid()) {
                    metaData.putValue("customerId", customerId.toString());
                }
                if (actionType == ActionType.ASSIGNED_TO_CUSTOMER) {
                    String strCustomerId = extractParameter(String.class, 1, additionalInfo);
                    String strCustomerName = extractParameter(String.class, 2, additionalInfo);
                    metaData.putValue("assignedCustomerId", strCustomerId);
                    metaData.putValue("assignedCustomerName", strCustomerName);
                } else if (actionType == ActionType.UNASSIGNED_FROM_CUSTOMER) {
                    String strCustomerId = extractParameter(String.class, 1, additionalInfo);
                    String strCustomerName = extractParameter(String.class, 2, additionalInfo);
                    metaData.putValue("unassignedCustomerId", strCustomerId);
                    metaData.putValue("unassignedCustomerName", strCustomerName);
                } else if (actionType == ActionType.ASSIGNED_FROM_TENANT) {
                    String strTenantId = extractParameter(String.class, 0, additionalInfo);
                    String strTenantName = extractParameter(String.class, 1, additionalInfo);
                    metaData.putValue("assignedFromTenantId", strTenantId);
                    metaData.putValue("assignedFromTenantName", strTenantName);
                } else if (actionType == ActionType.ASSIGNED_TO_TENANT) {
                    String strTenantId = extractParameter(String.class, 0, additionalInfo);
                    String strTenantName = extractParameter(String.class, 1, additionalInfo);
                    metaData.putValue("assignedToTenantId", strTenantId);
                    metaData.putValue("assignedToTenantName", strTenantName);
                } else if (actionType == ActionType.ASSIGNED_TO_EDGE) {
                    String strEdgeId = extractParameter(String.class, 1, additionalInfo);
                    String strEdgeName = extractParameter(String.class, 2, additionalInfo);
                    metaData.putValue("assignedEdgeId", strEdgeId);
                    metaData.putValue("assignedEdgeName", strEdgeName);
                } else if (actionType == ActionType.UNASSIGNED_FROM_EDGE) {
                    String strEdgeId = extractParameter(String.class, 1, additionalInfo);
                    String strEdgeName = extractParameter(String.class, 2, additionalInfo);
                    metaData.putValue("unassignedEdgeId", strEdgeId);
                    metaData.putValue("unassignedEdgeName", strEdgeName);
                } else if (actionType == ActionType.ADDED_COMMENT || actionType == ActionType.UPDATED_COMMENT) {
                    AlarmComment comment = extractParameter(AlarmComment.class, 0, additionalInfo);
                    metaData.putValue("comment", JacksonUtil.toString(comment));
                }
                ObjectNode entityNode;
                if (entity != null) {
                    entityNode = JacksonUtil.OBJECT_MAPPER.valueToTree(entity);
                    if (entityId.getEntityType() == EntityType.DASHBOARD) {
                        entityNode.put("configuration", "");
                    }
                    metaData.putValue("entityName", entity.getName());
                    metaData.putValue("entityType", entityId.getEntityType().toString());
                } else {
                    entityNode = JacksonUtil.newObjectNode();
                    if (actionType == ActionType.ATTRIBUTES_UPDATED) {
                        AttributeScope scope = extractParameter(AttributeScope.class, 0, additionalInfo);
                        @SuppressWarnings("unchecked")
                        List<AttributeKvEntry> attributes = extractParameter(List.class, 1, additionalInfo);
                        metaData.putValue(DataConstants.SCOPE, scope.name());
                        if (attributes != null) {
                            for (AttributeKvEntry attr : attributes) {
                                JacksonUtil.addKvEntry(entityNode, attr);
                            }
                        }
                    } else if (actionType == ActionType.ATTRIBUTES_DELETED) {
                        AttributeScope scope = extractParameter(AttributeScope.class, 0, additionalInfo);
                        @SuppressWarnings("unchecked")
                        List<String> keys = extractParameter(List.class, 1, additionalInfo);
                        metaData.putValue(DataConstants.SCOPE, scope.name());
                        ArrayNode attrsArrayNode = entityNode.putArray("attributes");
                        if (keys != null) {
                            keys.forEach(attrsArrayNode::add);
                        }
                    } else if (actionType == ActionType.TIMESERIES_UPDATED) {
                        @SuppressWarnings("unchecked")
                        List<TsKvEntry> timeseries = extractParameter(List.class, 0, additionalInfo);
                        addTimeseries(entityNode, timeseries);
                    } else if (actionType == ActionType.TIMESERIES_DELETED) {
                        @SuppressWarnings("unchecked")
                        List<String> keys = extractParameter(List.class, 0, additionalInfo);
                        if (keys != null) {
                            ArrayNode timeseriesArrayNode = entityNode.putArray("timeseries");
                            keys.forEach(timeseriesArrayNode::add);
                        }
                        entityNode.put("startTs", extractParameter(Long.class, 1, additionalInfo));
                        entityNode.put("endTs", extractParameter(Long.class, 2, additionalInfo));
                    } else if (ActionType.RELATION_ADD_OR_UPDATE.equals(actionType) || ActionType.RELATION_DELETED.equals(actionType)) {
                        entityNode = JacksonUtil.OBJECT_MAPPER.valueToTree(extractParameter(EntityRelation.class, 0, additionalInfo));
                    }
                }

                if (tenantId == null || tenantId.isNullUid()) {
                    if (entity instanceof HasTenantId) {
                        tenantId = ((HasTenantId) entity).getTenantId();
                    }
                }
                if (tenantId != null && !tenantId.isSysTenantId()) {
                    processNotificationRules(tenantId, entityId, entity, actionType, user, additionalInfo);
                }
                TbMsg tbMsg = TbMsg.newMsg()
                        .type(msgType.get())
                        .originator(entityId)
                        .customerId(customerId)
                        .copyMetaData(metaData)
                        .dataType(TbMsgDataType.JSON)
                        .data(JacksonUtil.toString(entityNode))
                        .build();
                tbClusterService.pushMsgToRuleEngine(tenantId, entityId, tbMsg, null);
            } catch (Exception e) {
                log.warn("[{}] Failed to push entity action to rule engine: {}", entityId, actionType, e);
            }
        }
    }

    /**
     * 按动作类型向 {@link NotificationRuleProcessor} 投递对应触发器。
     * <p>
     * {@code ADDED} 会先触发实体数量限额检查，再落入与 UPDATED/DELETED 相同的实体动作通知；
     * 告警分配 / 评论类动作要求实体分别为 {@link AlarmInfo} / {@link Alarm}，否则记警告并跳过。
     *
     * @param tenantId       租户（非系统租户）
     * @param originatorId   消息源实体 ID；若 {@code entity} 实现了 {@link HasId} 则优先用实体自身 ID
     * @param entity         实体快照
     * @param actionType     动作类型
     * @param user           操作用户，可为 {@code null}
     * @param additionalInfo 告警评论等附加参数
     */
    private void processNotificationRules(TenantId tenantId, EntityId originatorId, HasName entity, ActionType actionType, User user, Object... additionalInfo) {
        EntityId entityId = entity instanceof HasId ? ((HasId<? extends EntityId>) entity).getId() : originatorId;
        switch (actionType) {
            case ADDED:
                notificationRuleProcessor.process(EntitiesLimitTrigger.builder()
                        .tenantId(tenantId)
                        .entityType(entityId.getEntityType())
                        .build());
            case UPDATED:
            case DELETED:
                notificationRuleProcessor.process(EntityActionTrigger.builder()
                        .tenantId(tenantId)
                        .entityId(entityId)
                        .entity(entity)
                        .actionType(actionType)
                        .user(user)
                        .build());
                break;
            case ALARM_ASSIGNED:
            case ALARM_UNASSIGNED:
                if (!(entity instanceof AlarmInfo)) { // should not normally happen
                    log.warn("Invalid alarm assignment event: entity is not instance of AlarmInfo");
                    break;
                }
                notificationRuleProcessor.process(AlarmAssignmentTrigger.builder()
                        .tenantId(tenantId)
                        .alarmInfo((AlarmInfo) entity)
                        .actionType(actionType)
                        .user(user)
                        .build());
                break;
            case ADDED_COMMENT:
            case UPDATED_COMMENT:
                if (!(entity instanceof Alarm)) { // should not normally happen
                    log.warn("Invalid alarm comment event: entity is not instance of Alarm");
                    break;
                }
                notificationRuleProcessor.process(AlarmCommentTrigger.builder()
                        .tenantId(tenantId)
                        .comment(extractParameter(AlarmComment.class, 0, additionalInfo))
                        .alarm((Alarm) entity)
                        .actionType(actionType)
                        .user(user)
                        .build());
                break;
        }
    }

    /**
     * 有用户上下文时的标准落点：成功则先推规则引擎，再无论成败都写审计日志。
     * <p>
     * 若传入的 {@code customerId} 为空或 nullUid，则回落到 {@code user.getCustomerId()}。
     *
     * @param user           操作用户，不可为 {@code null}
     * @param entityId       实体 ID
     * @param entity         实体快照，可为 {@code null}
     * @param customerId     客户；空则用用户所属客户
     * @param actionType     动作类型
     * @param e              业务异常；{@code null} 表示成功并会推规则引擎
     * @param additionalInfo 附加参数，原样传给审计与规则引擎推送
     * @param <E>            实体类型
     * @param <I>            实体 ID 类型
     */
    public <E extends HasName, I extends EntityId> void logEntityAction(User user, I entityId, E entity, CustomerId customerId,
                                                                        ActionType actionType, Exception e, Object... additionalInfo) {
        if (customerId == null || customerId.isNullUid()) {
            customerId = user.getCustomerId();
        }
        if (e == null) {
            pushEntityActionToRuleEngine(entityId, entity, user.getTenantId(), customerId, actionType, user, additionalInfo);
        }
        auditLogService.logEntityAction(user.getTenantId(), customerId, user.getId(), user.getName(), entityId, entity, actionType, e, additionalInfo);
    }

    /**
     * 从可变参数中按类型与下标安全取出参数；越界、类型不符时返回 {@code null}。
     *
     * @param clazz          期望类型
     * @param index          下标（从 0 开始）
     * @param additionalInfo 附加参数数组
     * @param <T>            返回类型
     * @return 匹配的参数，否则 {@code null}
     */
    private <T> T extractParameter(Class<T> clazz, int index, Object... additionalInfo) {
        T result = null;
        if (additionalInfo != null && additionalInfo.length > index) {
            Object paramObject = additionalInfo[index];
            if (clazz.isInstance(paramObject)) {
                result = clazz.cast(paramObject);
            }
        }
        return result;
    }

    /**
     * 将时序条目按时间戳分组写入实体 JSON 的 {@code timeseries} 数组，
     * 每组形如 {@code { "ts": ..., "values": { key: value, ... } }}。
     *
     * @param entityNode 目标 JSON 节点
     * @param timeseries 时序键值列表；空或 null 时不写入
     */
    private void addTimeseries(ObjectNode entityNode, List<TsKvEntry> timeseries) {
        if (timeseries != null && !timeseries.isEmpty()) {
            ArrayNode result = entityNode.putArray("timeseries");
            Map<Long, List<TsKvEntry>> groupedTelemetry = timeseries.stream()
                    .collect(Collectors.groupingBy(TsKvEntry::getTs));
            for (Map.Entry<Long, List<TsKvEntry>> entry : groupedTelemetry.entrySet()) {
                ObjectNode element = JacksonUtil.newObjectNode();
                element.put("ts", entry.getKey());
                ObjectNode values = element.putObject("values");
                for (TsKvEntry tsKvEntry : entry.getValue()) {
                    JacksonUtil.addKvEntry(values, tsKvEntry);
                }
                result.add(element);
            }
        }
    }

}
