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
package org.thingsboard.server.service.entitiy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.HasName;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.service.action.EntityActionService;

/**
 * {@link TbLogEntityActionService} 的默认实现：将各重载收敛到完整参数形式，
 * 再按「是否有用户 / 是否有异常」分支委托 {@link EntityActionService}。
 * <p>
 * 分支策略：
 * <ul>
 *   <li>{@code user != null}：走 {@link EntityActionService#logEntityAction}，
 *       写审计日志；若无异常还会推送到规则引擎；</li>
 *   <li>{@code user == null} 且 {@code e == null}：仅
 *       {@link EntityActionService#pushEntityActionToRuleEngine}，
 *       用于无用户上下文的成功系统操作；</li>
 *   <li>{@code user == null} 且存在异常：不记审计、不推规则引擎。</li>
 * </ul>
 * 关系操作会对 from / to 两端各调用一次实体动作记录。
 *
 * @see TbLogEntityActionService
 * @see EntityActionService
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DefaultTbLogEntityActionService implements TbLogEntityActionService {

    /** 实际写审计日志、组装 TbMsg 并推规则引擎 / 通知规则的组件 */
    private final EntityActionService entityActionService;

    /**
     * {@inheritDoc}
     * <p>
     * 无实体、无客户，转完整重载。
     */
    @Override
    public <I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, ActionType actionType,
                                                     User user, Exception e, Object... additionalInfo) {
        logEntityAction(tenantId, entityId, null, null, actionType, user, e, additionalInfo);
    }

    /**
     * {@inheritDoc}
     * <p>
     * 无客户、无异常，转完整重载。
     */
    @Override
    public <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity,
                                                                        ActionType actionType, User user, Object... additionalInfo) {
        logEntityAction(tenantId, entityId, entity, null, actionType, user, null, additionalInfo);
    }

    /**
     * {@inheritDoc}
     * <p>
     * 无客户，转完整重载。
     */
    @Override
    public <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity,
                                                                        ActionType actionType, User user, Exception e,
                                                                        Object... additionalInfo) {
        logEntityAction(tenantId, entityId, entity, null, actionType, user, e, additionalInfo);
    }

    /**
     * {@inheritDoc}
     * <p>
     * 无异常，转完整重载。
     */
    @Override
    public <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity, CustomerId customerId,
                                                                        ActionType actionType, User user, Object... additionalInfo) {
        logEntityAction(tenantId, entityId, entity, customerId, actionType, user, null, additionalInfo);
    }

    /**
     * {@inheritDoc}
     * <p>
     * 按用户/异常分支委托 {@link EntityActionService}，见类级说明。
     */
    @Override
    public <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity,
                                                                        CustomerId customerId, ActionType actionType,
                                                                        User user, Exception e, Object... additionalInfo) {
        if (user != null) {
            entityActionService.logEntityAction(user, entityId, entity, customerId, actionType, e, additionalInfo);
        } else if (e == null) {
            entityActionService.pushEntityActionToRuleEngine(entityId, entity, tenantId, customerId, actionType, null, additionalInfo);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * 对关系两端实体各记一条动作。
     */
    @Override
    public void logEntityRelationAction(TenantId tenantId, CustomerId customerId, EntityRelation relation, User user,
                                        ActionType actionType, Exception e, Object... additionalInfo) {
        logEntityAction(tenantId, relation.getFrom(), null, customerId, actionType, user, e, additionalInfo);
        logEntityAction(tenantId, relation.getTo(), null, customerId, actionType, user, e, additionalInfo);
    }
}
