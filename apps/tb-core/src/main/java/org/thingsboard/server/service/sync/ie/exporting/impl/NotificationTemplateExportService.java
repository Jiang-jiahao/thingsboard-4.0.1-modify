package org.thingsboard.server.service.sync.ie.exporting.impl;

import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.NotificationTemplateId;
import org.thingsboard.server.common.data.notification.template.NotificationTemplate;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.service.sync.vc.data.EntitiesExportCtx;

import java.util.Set;

/**
 * 针对 {@link NotificationTemplate} 的导出服务，继承 {@link BaseEntityExportService}。
 * <p>
 * 模板无跨实体关联，{@link #setRelatedEntities} 为空实现。
 */
@Service
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
