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
 * 设备、资产、仪表板等增删改之后，在这里记一笔「谁动了什么」。
 *
 * <p>短参数的重载只是把缺的东西补成 null，最后都走到参数最全的那一个。
 * 真正写审计、通知规则引擎的是 {@link EntityActionService}。
 *
 * <p>有登录用户：记审计日志；成功时还会推给规则引擎。<br>
 * 没用户但成功了（系统自己改的）：只推规则引擎，不写审计。<br>
 * 没用户还失败了：什么都不做。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DefaultTbLogEntityActionService implements TbLogEntityActionService {

    private final EntityActionService entityActionService;

    /** 只有实体 id 和异常，例如删除失败时已经没有实体对象了。 */
    @Override
    public <I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, ActionType actionType,
                                                     User user, Exception e, Object... additionalInfo) {
        logEntityAction(tenantId, entityId, null, null, actionType, user, e, additionalInfo);
    }

    /** 成功操作：有实体对象，没有客户、没有异常。 */
    @Override
    public <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity,
                                                                        ActionType actionType, User user, Object... additionalInfo) {
        logEntityAction(tenantId, entityId, entity, null, actionType, user, null, additionalInfo);
    }

    /** 有实体对象和可选异常，没有单独传客户。 */
    @Override
    public <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity,
                                                                        ActionType actionType, User user, Exception e,
                                                                        Object... additionalInfo) {
        logEntityAction(tenantId, entityId, entity, null, actionType, user, e, additionalInfo);
    }

    /** 成功操作：有实体对象和客户。 */
    @Override
    public <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity, CustomerId customerId,
                                                                        ActionType actionType, User user, Object... additionalInfo) {
        logEntityAction(tenantId, entityId, entity, customerId, actionType, user, null, additionalInfo);
    }

    /**
     * 真正分岔的地方。
     * 有 user 就去写审计；没 user 且没异常就只通知规则引擎；又没 user 又失败则忽略。
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

    /** 关系有两端（从谁 → 到谁），两边各记一笔，两边的审计和规则都能看到这次改动。 */
    @Override
    public void logEntityRelationAction(TenantId tenantId, CustomerId customerId, EntityRelation relation, User user,
                                        ActionType actionType, Exception e, Object... additionalInfo) {
        logEntityAction(tenantId, relation.getFrom(), null, customerId, actionType, user, e, additionalInfo);
        logEntityAction(tenantId, relation.getTo(), null, customerId, actionType, user, e, additionalInfo);
    }
}
