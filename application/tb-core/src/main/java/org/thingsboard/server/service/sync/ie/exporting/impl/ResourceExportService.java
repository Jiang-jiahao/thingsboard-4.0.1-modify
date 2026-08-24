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
package org.thingsboard.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.TbResource;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.TbResourceId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link TbResource} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 清空 preview（导入时重新生成），不额外处理关联实体。
 */
@Service
@TbCoreComponent
public class ResourceExportService extends BaseEntityExportService<TbResourceId, TbResource, EntityExportData<TbResource>> {

    /**
     * 调用基类附加数据后清空 preview，避免把生成图写入 Git。
     */
    @Override
    protected void setAdditionalExportData(EntitiesExportCtx<?> ctx, TbResource resource, EntityExportData<TbResource> exportData) throws ThingsboardException {
        super.setAdditionalExportData(ctx, resource, exportData);
        resource.setPreview(null); // will be generated on import
    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.TB_RESOURCE);
    }

}
