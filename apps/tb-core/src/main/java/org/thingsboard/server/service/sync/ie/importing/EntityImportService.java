package org.thingsboard.server.service.sync.ie.importing;

import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.ExportableEntity;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.common.data.sync.ie.EntityImportResult;
import org.thingsboard.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 单一实体类型的导入服务。把 {@link EntityExportData} 按冲突策略写回当前租户。
 */
public interface EntityImportService<I extends EntityId, E extends ExportableEntity<I>, D extends EntityExportData<E>> {

    /**
     * 导入一条导出数据，返回创建/更新结果及后续引用回调。
     */
    EntityImportResult<E> importEntity(EntitiesImportCtx ctx, D exportData) throws ThingsboardException;

    /**
     * 本服务对应的实体类型。
     */
    EntityType getEntityType();

}
