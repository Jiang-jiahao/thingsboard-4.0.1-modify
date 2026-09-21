package com.jnks.iot.server.service.sync.ie.importing;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.ExportableEntity;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.common.data.sync.ie.EntityImportResult;
import com.jnks.iot.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 单一实体类型的导入服务。把 {@link EntityExportData} 按冲突策略写回当前租户。
 */
public interface EntityImportService<I extends EntityId, E extends ExportableEntity<I>, D extends EntityExportData<E>> {

    /**
     * 导入一条导出数据，返回创建/更新结果及后续引用回调。
     */
    EntityImportResult<E> importEntity(EntitiesImportCtx ctx, D exportData) throws JnksIotException;

    /**
     * 本服务对应的实体类型。
     */
    EntityType getEntityType();

}
