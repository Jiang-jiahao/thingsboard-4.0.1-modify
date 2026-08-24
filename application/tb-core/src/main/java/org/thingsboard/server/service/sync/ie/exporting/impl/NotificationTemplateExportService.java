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
import org.thingsboard.server.common.data.id.NotificationTemplateId;
import org.thingsboard.server.common.data.notification.template.NotificationTemplate;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link NotificationTemplate} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 模板无跨实体关联，{@link #setRelatedEntities} 为空实现。
 */
@Service
@TbCoreComponent
public class NotificationTemplateExportService extends BaseEntityExportService<NotificationTemplateId, NotificationTemplate, EntityExportData<NotificationTemplate>> {

    /** 模板覆盖：通知模板无关联实体需要替换。 */
    @Override
    protected void setRelatedEntities(EntitiesExportCtx<?> ctx, NotificationTemplate notificationTemplate, EntityExportData<NotificationTemplate> exportData) {

    }

    @Override
    public Set<EntityType> getSupportedEntityTypes() {
        return Set.of(EntityType.NOTIFICATION_TEMPLATE);
    }

}
