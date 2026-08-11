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

import org.thingsboard.server.common.data.HasName;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.relation.EntityRelation;

/**
 * 实体操作审计与规则引擎通知的统一入口。
 * <p>
 * 业务层（设备/资产/仪表板等 CRUD、属性与关系变更）在完成数据变更后，
 * 通过本接口记录操作轨迹。实现类会按是否存在操作用户，决定走
 * 「审计日志 + 推规则引擎」还是「仅推规则引擎」等路径，细节见
 * {@link DefaultTbLogEntityActionService} 与 {@link org.thingsboard.server.service.action.EntityActionService}。
 * <p>
 * 多个重载用于逐步补齐可选参数：实体快照、客户、异常、附加信息等。
 * {@code additionalInfo} 的语义随 {@link ActionType} 变化（如分配客户时的客户 id/名称、
 * 属性更新时的 scope 与键值列表），由下游 {@code EntityActionService} 按动作类型解析。
 *
 * @see DefaultTbLogEntityActionService
 * @see org.thingsboard.server.service.action.EntityActionService
 */
public interface TbLogEntityActionService {

    /**
     * 记录实体操作（无实体快照、无客户），通常用于删除失败等仅有 id 与异常的场景。
     *
     * @param tenantId       租户 ID
     * @param entityId       被操作实体 ID
     * @param actionType     操作类型（增删改、分配、属性变更等）
     * @param user           操作用户；可为 {@code null}（系统/无用户上下文）
     * @param e              失败时的异常；成功时传 {@code null}
     * @param additionalInfo 随动作类型变化的附加参数
     * @param <I>            实体 ID 类型
     */
    <I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, ActionType actionType, User user,
                                              Exception e, Object... additionalInfo);

    /**
     * 记录实体操作（带实体快照，无客户、无异常）。
     *
     * @param tenantId       租户 ID
     * @param entityId       被操作实体 ID
     * @param entity         操作后（或操作时）的实体快照，需实现 {@link HasName}；可为 {@code null}
     * @param actionType     操作类型
     * @param user           操作用户；可为 {@code null}
     * @param additionalInfo 附加参数
     * @param <E>            实体类型
     * @param <I>            实体 ID 类型
     */
    <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity, ActionType actionType,
                                                                 User user, Object... additionalInfo);

    /**
     * 记录实体操作（带实体快照与可选异常，无显式客户）。
     *
     * @param tenantId       租户 ID
     * @param entityId       被操作实体 ID
     * @param entity         实体快照；可为 {@code null}
     * @param actionType     操作类型
     * @param user           操作用户；可为 {@code null}
     * @param e              失败异常；成功为 {@code null}
     * @param additionalInfo 附加参数
     * @param <E>            实体类型
     * @param <I>            实体 ID 类型
     */
    <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity, ActionType actionType,
                                                                 User user, Exception e, Object... additionalInfo);

    /**
     * 记录实体操作（带实体快照与客户，无异常）。
     *
     * @param tenantId       租户 ID
     * @param entityId       被操作实体 ID
     * @param entity         实体快照；可为 {@code null}
     * @param customerId     关联客户；可为 {@code null}（实现中可能回落到用户所属客户）
     * @param actionType     操作类型
     * @param user           操作用户；可为 {@code null}
     * @param additionalInfo 附加参数
     * @param <E>            实体类型
     * @param <I>            实体 ID 类型
     */
    <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity, CustomerId customerId,
                                                                 ActionType actionType, User user, Object... additionalInfo);

    /**
     * 记录实体操作的完整参数形式：实体、客户、用户、异常与附加信息均可指定。
     * <p>
     * 其它重载最终都会收敛到实现类中的等价完整调用。
     *
     * @param tenantId       租户 ID
     * @param entityId       被操作实体 ID
     * @param entity         实体快照；可为 {@code null}
     * @param customerId     关联客户；可为 {@code null}
     * @param actionType     操作类型
     * @param user           操作用户；可为 {@code null}
     * @param e              失败异常；成功为 {@code null}
     * @param additionalInfo 附加参数
     * @param <E>            实体类型
     * @param <I>            实体 ID 类型
     */
    <E extends HasName, I extends EntityId> void logEntityAction(TenantId tenantId, I entityId, E entity, CustomerId customerId,
                                                                 ActionType actionType, User user, Exception e,
                                                                 Object... additionalInfo);

    /**
     * 记录实体关系（Relation）上的操作。
     * <p>
     * 实现通常会对关系的 {@code from}、{@code to} 两端各记一条实体动作，
     * 便于审计与规则引擎两侧都能感知关系变更。
     *
     * @param tenantId       租户 ID
     * @param customerId     关联客户；可为 {@code null}
     * @param relation       被操作的关系
     * @param user           操作用户；可为 {@code null}
     * @param actionType     操作类型（如关系新增/更新/删除）
     * @param e              失败异常；成功为 {@code null}
     * @param additionalInfo 附加参数（常含关系对象本身等）
     */
    void logEntityRelationAction(TenantId tenantId, CustomerId customerId, EntityRelation relation, User user,
                                 ActionType actionType, Exception e, Object... additionalInfo);
}
