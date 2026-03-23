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
package org.thingsboard.server.service.edge;

import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.cluster.TbClusterService;
import org.thingsboard.server.common.data.EdgeUtils;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.OtaPackageInfo;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.alarm.Alarm;
import org.thingsboard.server.common.data.alarm.AlarmApiCallResult;
import org.thingsboard.server.common.data.alarm.AlarmComment;
import org.thingsboard.server.common.data.alarm.EntityAlarm;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.domain.Domain;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.edge.EdgeEventActionType;
import org.thingsboard.server.common.data.edge.EdgeEventType;
import org.thingsboard.server.common.data.id.RuleChainId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.data.rule.RuleChainType;
import org.thingsboard.server.common.data.security.Authority;
import org.thingsboard.server.dao.edge.EdgeSynchronizationManager;
import org.thingsboard.server.dao.eventsourcing.ActionEntityEvent;
import org.thingsboard.server.dao.eventsourcing.DeleteEntityEvent;
import org.thingsboard.server.dao.eventsourcing.RelationActionEvent;
import org.thingsboard.server.dao.eventsourcing.SaveEntityEvent;
import org.thingsboard.server.dao.tenant.TenantService;

/**
 * This event listener does not support async event processing because relay on ThreadLocal
 * Another possible approach is to implement a special annotation and a bunch of classes similar to TransactionalApplicationListener
 * This class is the simplest approach to maintain edge synchronization within the single class.
 * <p>
 * For async event publishers, you have to decide whether publish event on creating async task in the same thread where dao method called
 * @Autowired
 * EdgeEventSynchronizationManager edgeSynchronizationManager
 * ...
 *   //some async write action make future
 *   if (!edgeSynchronizationManager.isSync()) {
 *     future.addCallback(eventPublisher.publishEvent(...))
 *   }
 * */
@Slf4j
@Component
@RequiredArgsConstructor
public class EdgeEventSourcingListener {

    // 集群服务接口，用于向边缘发送通知
    private final TbClusterService tbClusterService;

    // 租户服务，用于验证租户是否存在
    private final TenantService tenantService;

    // 边缘同步管理器（ThreadLocal实现），提供当前边缘ID
    private final EdgeSynchronizationManager edgeSynchronizationManager;

    @PostConstruct
    public void init() {
        log.debug("EdgeEventSourcingListener initiated");
    }

    @TransactionalEventListener(fallbackExecution = true)
    public void handleEvent(SaveEntityEvent<?> event) {
        // 如果不是广播事件，则直接返回
        if (Boolean.FALSE.equals(event.getBroadcastEvent())) {
            log.trace("Ignoring event {}", event);
            return;
        }

        try {
            // 验证是否为需要处理的保存事件
            if (!isValidSaveEntityEventForEdgeProcessing(event)) {
                return;
            }
            log.trace("[{}] SaveEntityEvent called: {}", event.getTenantId(), event);
            // 判断是创建还是更新操作
            boolean isCreated = Boolean.TRUE.equals(event.getCreated());
            // 获取事件体（JSON字符串）
            String body = getBodyMsgForEntityEvent(event.getEntity());
            // 获取边缘事件类型
            EdgeEventType type = getEdgeEventTypeForEntityEvent(event.getEntity());
            // 获取边缘事件动作类型
            EdgeEventActionType action = getActionForEntityEvent(event.getEntity(), isCreated);
            // 发送边缘通知（从ThreadLocal获取当前边缘ID）
            tbClusterService.sendNotificationMsgToEdge(event.getTenantId(), null, event.getEntityId(),
                    body, type, action, edgeSynchronizationManager.getEdgeId().get());
        } catch (Exception e) {
            log.error("[{}] failed to process SaveEntityEvent: {}", event.getTenantId(), event, e);
        }
    }

    @TransactionalEventListener(fallbackExecution = true)
    public void handleEvent(DeleteEntityEvent<?> event) {
        TenantId tenantId = event.getTenantId();
        EntityType entityType = event.getEntityId().getEntityType();
        // 验证租户是否存在（系统租户除外）
        if (!tenantId.isSysTenantId() && !tenantService.tenantExists(tenantId)) {
            log.debug("[{}] Ignoring DeleteEntityEvent because tenant does not exist: {}", tenantId, event);
            return;
        }
        try {
            // 跳过特定类型的删除事件
            if (EntityType.TENANT.equals(entityType) || EntityType.EDGE.equals(entityType)) {
                return;
            }
            log.trace("[{}] DeleteEntityEvent called: {}", tenantId, event);
            // 获取边缘事件类型
            EdgeEventType type = getEdgeEventTypeForEntityEvent(event.getEntity());
            // 获取删除动作类型（区分普通删除/告警删除/评论删除）
            EdgeEventActionType actionType = getEdgeEventActionTypeForEntityEvent(event.getEntity());
            // 发送边缘通知
            tbClusterService.sendNotificationMsgToEdge(tenantId, null, event.getEntityId(),
                    JacksonUtil.toString(event.getEntity()), type, actionType,
                    edgeSynchronizationManager.getEdgeId().get());
        } catch (Exception e) {
            log.error("[{}] failed to process DeleteEntityEvent: {}", tenantId, event, e);
        }
    }

    /**
     * 获取删除事件的专用动作类型
     * 区分普通删除/告警删除/评论删除
     */
    private EdgeEventActionType getEdgeEventActionTypeForEntityEvent(Object entity) {
        if (entity instanceof AlarmComment) {
            return EdgeEventActionType.DELETED_COMMENT;
        } else if (entity instanceof Alarm) {
            return EdgeEventActionType.ALARM_DELETE;
        }
        return EdgeEventActionType.DELETED;
    }

    /**
     * 处理实体动作事件（如分配操作）
     * 过滤条件：
     *   - 设备分配给租户的事件
     *   - 告警事件
     *   - 边缘根规则链的分配事件（防循环）
     */
    @TransactionalEventListener(fallbackExecution = true)
    public void handleEvent(ActionEntityEvent<?> event) {
        // 跳过特定类型的动作事件
        if (EntityType.DEVICE.equals(event.getEntityId().getEntityType()) && ActionType.ASSIGNED_TO_TENANT.equals(event.getActionType())) {
            return;
        }
        if (EntityType.ALARM.equals(event.getEntityId().getEntityType())) {
            return;
        }
        try {
            // 特殊处理规则链分配边缘事件
            if (event.getEntityId().getEntityType().equals(EntityType.RULE_CHAIN) && event.getEdgeId() != null && event.getActionType().equals(ActionType.ASSIGNED_TO_EDGE)) {
                try {
                    // 检查是否为边缘根规则链（避免循环同步）
                    Edge edge = JacksonUtil.fromString(event.getBody(), Edge.class);
                    if (edge != null && new RuleChainId(event.getEntityId().getId()).equals(edge.getRootRuleChainId())) {
                        log.trace("[{}] skipping ASSIGNED_TO_EDGE event of RULE_CHAIN entity in case Edge Root Rule Chain: {}", event.getTenantId(), event);
                        return;
                    }
                } catch (Exception ignored) {
                    return;
                }
            }
            log.trace("[{}] ActionEntityEvent called: {}", event.getTenantId(), event);
            // 发送边缘通知（转换动作类型）
            tbClusterService.sendNotificationMsgToEdge(event.getTenantId(), event.getEdgeId(), event.getEntityId(),
                    event.getBody(), null, EdgeUtils.getEdgeEventActionTypeByActionType(event.getActionType()),
                    edgeSynchronizationManager.getEdgeId().get());
        } catch (Exception e) {
            log.error("[{}] failed to process ActionEntityEvent: {}", event.getTenantId(), event, e);
        }
    }

    /**
     * 处理关系动作事件
     * 过滤条件：
     *   - 租户不存在（删除关系时）
     *   - 非COMMON类型的关系
     */
    @TransactionalEventListener(fallbackExecution = true)
    public void handleEvent(RelationActionEvent event) {
        try {
            TenantId tenantId = event.getTenantId();
            // 删除关系时验证租户存在性
            if (ActionType.RELATION_DELETED.equals(event.getActionType()) && !tenantService.tenantExists(tenantId)) {
                log.debug("[{}] Ignoring RelationActionEvent because tenant does not exist: {}", tenantId, event);
                return;
            }
            // 跳过空关系或非COMMON分组的关系
            EntityRelation relation = event.getRelation();
            if (relation == null) {
                log.trace("[{}] skipping RelationActionEvent event in case relation is null: {}", event.getTenantId(), event);
                return;
            }
            if (!RelationTypeGroup.COMMON.equals(relation.getTypeGroup())) {
                log.trace("[{}] skipping RelationActionEvent event in case NOT COMMON relation type group: {}", event.getTenantId(), event);
                return;
            }
            log.trace("[{}] RelationActionEvent called: {}", event.getTenantId(), event);
            // 发送关系事件通知
            tbClusterService.sendNotificationMsgToEdge(event.getTenantId(), null, null,
                    JacksonUtil.toString(relation), EdgeEventType.RELATION, EdgeUtils.getEdgeEventActionTypeByActionType(event.getActionType()),
                    edgeSynchronizationManager.getEdgeId().get());
        } catch (Exception e) {
            log.error("[{}] failed to process RelationActionEvent: {}", event.getTenantId(), event, e);
        }
    }

    /**
     * 验证保存事件是否需要处理
     * 按实体类型进行特殊过滤：
     *   - RULE_CHAIN: 仅处理EDGE类型的规则链
     *   - USER: 跳过SYS_ADMIN；更新时需检查实际变更
     *   - OTA_PACKAGE: 必须有URL或数据
     *   - ALARM: 跳过AlarmApiCallResult/Alarm/EntityAlarm
     *   - TENANT: 仅处理更新（非创建）
     *   - API_USAGE_STATE/EDGE: 完全跳过
     *   - DOMAIN: 根据propagateToEdge标志决定
     */
    private boolean isValidSaveEntityEventForEdgeProcessing(SaveEntityEvent<?> event) {
        Object entity = event.getEntity();
        Object oldEntity = event.getOldEntity();
        if (event.getEntityId() != null) {
            switch (event.getEntityId().getEntityType()) {
                case RULE_CHAIN:
                    // 仅同步EDGE类型的规则链
                    if (entity instanceof RuleChain ruleChain) {
                        return RuleChainType.EDGE.equals(ruleChain.getType());
                    }
                    break;
                case USER:
                    if (entity instanceof User user) {
                        // 跳过系统管理员
                        if (Authority.SYS_ADMIN.equals(user.getAuthority())) {
                            return false;
                        }
                        // 更新时检查实际字段变更（忽略附加信息空值）
                        if (oldEntity != null) {
                            user = JacksonUtil.clone(user);
                            User oldUser = JacksonUtil.clone((User) oldEntity);
                            cleanUpUserAdditionalInfo(oldUser);
                            cleanUpUserAdditionalInfo(user);
                            return !user.equals(oldUser);
                        }
                    }
                    break;
                case OTA_PACKAGE:
                    // 仅处理有效OTA包
                    if (entity instanceof OtaPackageInfo otaPackageInfo) {
                        return otaPackageInfo.hasUrl() || otaPackageInfo.isHasData();
                    }
                    break;
                case ALARM:
                    // 跳过告警相关内部事件
                    if (entity instanceof AlarmApiCallResult || entity instanceof Alarm || entity instanceof EntityAlarm) {
                        return false;
                    }
                    break;
                case TENANT:
                    // 仅处理租户更新（创建由其他逻辑处理）
                    return !event.getCreated();
                case API_USAGE_STATE, EDGE:
                    // 明确跳过类型
                    return false;
                case DOMAIN:
                    if (entity instanceof Domain domain) {
                        return domain.isPropagateToEdge();
                    }
            }
        }
        // Default: If the entity doesn't match any of the conditions, consider it as valid.
        return true;
    }

    /**
     * 清理用户附加信息中的空值
     * 目的：在比较用户更新时忽略空附加信息的影响
     */
    private void cleanUpUserAdditionalInfo(User user) {
        if (user.getAdditionalInfo() instanceof NullNode) {
            user.setAdditionalInfo(null);
        }
        if (user.getAdditionalInfo() instanceof ObjectNode additionalInfo) {
            if (additionalInfo.isEmpty()) {
                user.setAdditionalInfo(null);
            } else {
                user.setAdditionalInfo(additionalInfo);
            }
        }
        user.setVersion(null);
    }

    /**
     * 获取实体对应的边缘事件类型
     * 当前仅告警评论有特殊类型
     */
    private EdgeEventType getEdgeEventTypeForEntityEvent(Object entity) {
        if (entity instanceof AlarmComment) {
            return EdgeEventType.ALARM_COMMENT;
        }
        return null;
    }

    /**
     * 获取事件体JSON
     * 当前仅告警评论需要特殊序列化
     */
    private String getBodyMsgForEntityEvent(Object entity) {
        if (entity instanceof AlarmComment) {
            return JacksonUtil.toString(entity);
        }
        return null;
    }

    /**
     * 根据操作类型（创建/更新）获取边缘动作
     * 告警评论有专用动作类型
     */
    private EdgeEventActionType getActionForEntityEvent(Object entity, boolean isCreated) {
        if (entity instanceof AlarmComment) {
            return isCreated ? EdgeEventActionType.ADDED_COMMENT : EdgeEventActionType.UPDATED_COMMENT;
        }
        return isCreated ? EdgeEventActionType.ADDED : EdgeEventActionType.UPDATED;
    }

}
