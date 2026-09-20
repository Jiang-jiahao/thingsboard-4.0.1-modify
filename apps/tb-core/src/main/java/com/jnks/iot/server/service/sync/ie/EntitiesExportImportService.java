package com.jnks.iot.server.service.sync.ie;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.ExportableEntity;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.common.data.sync.ie.EntityImportResult;
import com.jnks.iot.server.service.sync.vc.data.EntitiesExportCtx;
import com.jnks.iot.server.service.sync.vc.data.EntitiesImportCtx;

import java.util.Comparator;

/**
 * 实体导入/导出门面。
 * <p>
 * 按实体类型分发到对应的 {@link com.jnks.iot.server.service.sync.ie.exporting.EntityExportService}
 * / {@link com.jnks.iot.server.service.sync.ie.importing.EntityImportService}；
 * 版本控制从 Git 取出 JSON 后也走本接口完成落库与关联修复。
 */
public interface EntitiesExportImportService {

    /**
     * 将指定实体导出为可序列化的 {@link EntityExportData}（含关联、属性、计算字段等可选数据）。
     */
    <E extends ExportableEntity<I>, I extends EntityId> EntityExportData<E> exportEntity(EntitiesExportCtx<?> ctx, I entityId) throws JnksIotException;

    /**
     * 按冲突策略导入一条导出数据：匹配已有实体则更新，否则新建；并登记外部 ID 到内部 ID 的映射。
     */
    <E extends ExportableEntity<I>, I extends EntityId> EntityImportResult<E> importEntity(EntitiesImportCtx ctx, EntityExportData<E> exportData) throws JnksIotException;

    /**
     * 在全部实体导入完成后，统一执行引用回调并保存关系。
     */
    void saveReferencesAndRelations(EntitiesImportCtx ctx) throws JnksIotException;

    /**
     * 导入时实体类型的依赖排序（先客户/规则链/资源，后设备/仪表板等）。
     */
    Comparator<EntityType> getEntityTypeComparatorForImport();

}
