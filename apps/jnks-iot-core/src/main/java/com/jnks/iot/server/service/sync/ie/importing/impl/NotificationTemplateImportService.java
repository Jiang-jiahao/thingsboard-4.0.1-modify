package com.jnks.iot.server.service.sync.ie.importing.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.NotificationTemplateId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.template.NotificationTemplate;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.dao.notification.NotificationTemplateService;
import com.jnks.iot.server.dao.service.ConstraintValidator;
import com.jnks.iot.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link NotificationTemplate} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 无额外关联映射，校验字段后按租户保存模板。
 */
@Service
@RequiredArgsConstructor
public class NotificationTemplateImportService extends BaseEntityImportService<NotificationTemplateId, NotificationTemplate, EntityExportData<NotificationTemplate>> {

    private final NotificationTemplateService notificationTemplateService;

    @Override
    protected void setOwner(TenantId tenantId, NotificationTemplate notificationTemplate, IdProvider idProvider) {
        notificationTemplate.setTenantId(tenantId);
    }

    /** 模板覆盖：通知模板无额外关联 ID 需要映射。 */
    @Override
    protected NotificationTemplate prepare(EntitiesImportCtx ctx, NotificationTemplate notificationTemplate, NotificationTemplate oldEntity, EntityExportData<NotificationTemplate> exportData, IdProvider idProvider) {
        return notificationTemplate;
    }

    /** 校验字段后保存通知模板。 */
    @Override
    protected NotificationTemplate saveOrUpdate(EntitiesImportCtx ctx, NotificationTemplate notificationTemplate, EntityExportData<NotificationTemplate> exportData, IdProvider idProvider) {
        ConstraintValidator.validateFields(notificationTemplate);
        return notificationTemplateService.saveNotificationTemplate(ctx.getTenantId(), notificationTemplate);
    }

    @Override
    protected void onEntitySaved(User user, NotificationTemplate savedEntity, NotificationTemplate oldEntity) throws JnksIotException {
        entityActionService.logEntityAction(user, savedEntity.getId(), savedEntity, null,
                oldEntity == null ? ActionType.ADDED : ActionType.UPDATED, null);
    }

    @Override
    protected NotificationTemplate deepCopy(NotificationTemplate notificationTemplate) {
        return new NotificationTemplate(notificationTemplate);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.NOTIFICATION_TEMPLATE;
    }

}
