package com.jnks.iot.server.service.sync.ie.exporting;

import com.jnks.iot.server.common.data.ExportableEntity;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.service.sync.vc.data.EntitiesExportCtx;

/**
 * 单一实体类型的导出服务。把数据库实体转换成可写入 Git 的 {@link EntityExportData}。
 */
public interface EntityExportService<I extends EntityId, E extends ExportableEntity<I>, D extends EntityExportData<E>> {

    /**
     * 按导出上下文设置，组装指定实体的导出数据包。
     */
    D getExportData(EntitiesExportCtx<?> ctx, I entityId) throws JnksIotException;

}
