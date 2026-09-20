package com.jnks.iot.server.dao.sql.notification;

import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.NotificationTemplateId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationType;
import com.jnks.iot.server.common.data.notification.template.NotificationTemplate;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.model.sql.NotificationTemplateEntity;
import com.jnks.iot.server.dao.notification.NotificationTemplateDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Component
@SqlDao
@RequiredArgsConstructor
public class JpaNotificationTemplateDao extends JpaAbstractDao<NotificationTemplateEntity, NotificationTemplate> implements NotificationTemplateDao, TenantEntityDao<NotificationTemplate> {

    private final NotificationTemplateRepository notificationTemplateRepository;

    @Override
    protected Class<NotificationTemplateEntity> getEntityClass() {
        return NotificationTemplateEntity.class;
    }

    @Override
    public PageData<NotificationTemplate> findByTenantIdAndNotificationTypesAndPageLink(TenantId tenantId, List<NotificationType> notificationTypes, PageLink pageLink) {
        return DaoUtil.toPageData(notificationTemplateRepository.findByTenantIdAndNotificationTypesAndSearchText(tenantId.getId(),
                notificationTypes, pageLink.getTextSearch(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public int countByTenantIdAndNotificationTypes(TenantId tenantId, Collection<NotificationType> notificationTypes) {
        return notificationTemplateRepository.countByTenantIdAndNotificationTypes(tenantId.getId(), notificationTypes);
    }

    @Override
    public void removeByTenantId(TenantId tenantId) {
        notificationTemplateRepository.deleteByTenantId(tenantId.getId());
    }

    @Override
    public NotificationTemplate findByTenantIdAndExternalId(UUID tenantId, UUID externalId) {
        return DaoUtil.getData(notificationTemplateRepository.findByTenantIdAndExternalId(tenantId, externalId));
    }

    @Override
    public NotificationTemplate findByTenantIdAndName(UUID tenantId, String name) {
        return DaoUtil.getData(notificationTemplateRepository.findByTenantIdAndName(tenantId, name));
    }

    @Override
    public PageData<NotificationTemplate> findByTenantId(UUID tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(notificationTemplateRepository.findByTenantId(tenantId, DaoUtil.toPageable(pageLink)));
    }

    @Override
    public NotificationTemplateId getExternalIdByInternal(NotificationTemplateId internalId) {
        return DaoUtil.toEntityId(notificationTemplateRepository.getExternalIdByInternal(internalId.getId()), NotificationTemplateId::new);
    }

    @Override
    public PageData<NotificationTemplate> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return findByTenantId(tenantId.getId(), pageLink);
    }

    @Override
    protected JpaRepository<NotificationTemplateEntity, UUID> getRepository() {
        return notificationTemplateRepository;
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.NOTIFICATION_TEMPLATE;
    }

}
