package com.jnks.iot.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.JnksIotResource;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.JnksIotResourceId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link JnksIotResource} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 清空 preview（导入时重新生成），不额外处理关联实体。
 */
@Service
public class ResourceExportService extends BaseEntityExportService<JnksIotResourceId, JnksIotResource, EntityExportData<JnksIotResource>> {

    /**
     * 调用基类附加数据后清空 preview，避免把生成图写入 Git。
     */
    @Override
    protected void setAdditionalExportData(EntitiesExportCtx<?> ctx, JnksIotResource resource, EntityExportData<JnksIotResource> exportData) throws JnksIotException {
        super.setAdditionalExportData(ctx, resource, exportData);
        resource.setPreview(null); // will be generated on import
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.JNKS_IOT_RESOURCE);
    }

}
