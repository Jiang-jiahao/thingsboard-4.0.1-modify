package org.thingsboard.server.service.sync.ie.exporting;

import org.thingsboard.server.common.data.ExportableEntity;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

/**
 * 单一实体类型的导出服务。把数据库实体转换成可写入 Git 的 {@link EntityExportData}。
 */
public interface EntityExportService<I extends EntityId, E extends ExportableEntity<I>, D extends EntityExportData<E>> {

    /**
     * 按导出上下文设置，组装指定实体的导出数据包。
     */
    D getExportData(EntitiesExportCtx<?> ctx, I entityId) throws ThingsboardException;

}
