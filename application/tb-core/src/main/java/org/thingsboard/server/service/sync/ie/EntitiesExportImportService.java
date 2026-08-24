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
package org.thingsboard.server.service.sync.ie;

import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.ExportableEntity;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.common.data.sync.ie.EntityImportResult;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;
import org.thingsboard.server.service.sync.vc.data.EntitiesImportCtx;

import java.util.Comparator;

/**
 * 实体导入/导出门面。
 * <p>
 * 按实体类型分发到对应的 {@link org.thingsboard.server.service.sync.ie.exporting.EntityExportService}
 * / {@link org.thingsboard.server.service.sync.ie.importing.EntityImportService}；
 * 版本控制从 Git 取出 JSON 后也走本接口完成落库与关联修复。
 */
public interface EntitiesExportImportService {

    /**
     * 将指定实体导出为可序列化的 {@link EntityExportData}（含关联、属性、计算字段等可选数据）。
     */
    <E extends ExportableEntity<I>, I extends EntityId> EntityExportData<E> exportEntity(EntitiesExportCtx<?> ctx, I entityId) throws ThingsboardException;

    /**
     * 按冲突策略导入一条导出数据：匹配已有实体则更新，否则新建；并登记外部 ID 到内部 ID 的映射。
     */
    <E extends ExportableEntity<I>, I extends EntityId> EntityImportResult<E> importEntity(EntitiesImportCtx ctx, EntityExportData<E> exportData) throws ThingsboardException;

    /**
     * 在全部实体导入完成后，统一执行引用回调并保存关系。
     */
    void saveReferencesAndRelations(EntitiesImportCtx ctx) throws ThingsboardException;

    /**
     * 导入时实体类型的依赖排序（先客户/规则链/资源，后设备/仪表板等）。
     */
    Comparator<EntityType> getEntityTypeComparatorForImport();

}
