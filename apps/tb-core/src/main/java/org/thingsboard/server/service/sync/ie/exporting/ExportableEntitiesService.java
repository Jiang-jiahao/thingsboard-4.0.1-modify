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
package org.thingsboard.server.service.sync.ie.exporting;

import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.ExportableEntity;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.HasId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;

/**
 * 可导出实体的统一 DAO 门面：按租户 + externalId / 内部 ID / 名称查找，并支持分页列出与删除。
 * 供导出、导入匹配已有实体、版本加载后清理「版本中不存在的实体」使用。
 */
public interface ExportableEntitiesService {

    /**
     * 按租户 + externalId 查找可导出实体。
     */
    <E extends ExportableEntity<I>, I extends EntityId> E findEntityByTenantIdAndExternalId(TenantId tenantId, I externalId);

    <E extends HasId<I>, I extends EntityId> E findEntityByTenantIdAndId(TenantId tenantId, I id);

    <E extends HasId<I>, I extends EntityId> E findEntityById(I id);

    <E extends ExportableEntity<I>, I extends EntityId> E findEntityByTenantIdAndName(TenantId tenantId, EntityType entityType, String name);

    <E extends ExportableEntity<I>, I extends EntityId> E findDefaultEntityByTenantId(TenantId tenantId, EntityType entityType);

    <E extends ExportableEntity<I>, I extends EntityId> PageData<E> findEntitiesByTenantId(TenantId tenantId, EntityType entityType, PageLink pageLink);

    <I extends EntityId> PageData<I> findEntitiesIdsByTenantId(TenantId tenantId, EntityType entityType, PageLink pageLink);

    /**
     * 内部 ID 转 externalId，供导出时替换关联引用。
     */
    <I extends EntityId> I getExternalIdByInternal(I internalId);

    /**
     * 按 ID 删除实体（版本加载时清理多余实体）。
     */
    <I extends EntityId> void removeById(TenantId tenantId, I id);

}
