package com.jnks.iot.server.dao.sql.notification;

import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.NotificationRuleId;
import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.NotificationRule;
import com.jnks.iot.server.common.data.notification.rule.NotificationRuleInfo;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.model.sql.NotificationRuleEntity;
import com.jnks.iot.server.dao.model.sql.NotificationRuleInfoEntity;
import com.jnks.iot.server.dao.notification.NotificationRuleDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@SqlDao
@RequiredArgsConstructor
public class JpaNotificationRuleDao extends JpaAbstractDao<NotificationRuleEntity, NotificationRule> implements NotificationRuleDao, TenantEntityDao<NotificationRule> {

    private final NotificationRuleRepository notificationRuleRepository;

    @Override
    public PageData<NotificationRule> findByTenantIdAndPageLink(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(notificationRuleRepository.findByTenantIdAndSearchText(tenantId.getId(),
                pageLink.getTextSearch(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<NotificationRuleInfo> findInfosByTenantIdAndPageLink(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.pageToPageData(notificationRuleRepository.findInfosByTenantIdAndSearchText(tenantId.getId(),
                        pageLink.getTextSearch(), DaoUtil.toPageable(pageLink, Map.of(
                                "templateName", "t.name"
                        ))))
                .mapData(NotificationRuleInfoEntity::toData);
    }

    @Override
    public boolean existsByTenantIdAndTargetId(TenantId tenantId, NotificationTargetId targetId) {
        return notificationRuleRepository.existsByTenantIdAndRecipientsConfigContaining(tenantId.getId(), targetId.getId().toString());
    }

    @Override
    public List<NotificationRule> findByTenantIdAndTriggerTypeAndEnabled(TenantId tenantId, NotificationRuleTriggerType triggerType, boolean enabled) {
        return DaoUtil.convertDataList(notificationRuleRepository.findAllByTenantIdAndTriggerTypeAndEnabled(tenantId.getId(), triggerType, enabled));
    }

    @Override
    public NotificationRuleInfo findInfoById(TenantId tenantId, NotificationRuleId id) {
        NotificationRuleInfoEntity infoEntity = notificationRuleRepository.findInfoById(id.getId());
        return infoEntity != null ? infoEntity.toData() : null;
    }

    @Override
    public void removeByTenantId(TenantId tenantId) {
        notificationRuleRepository.deleteByTenantId(tenantId.getId());
    }

    @Override
    public NotificationRule findByTenantIdAndExternalId(UUID tenantId, UUID externalId) {
        return DaoUtil.getData(notificationRuleRepository.findByTenantIdAndExternalId(tenantId, externalId));
    }

    @Override
    public NotificationRule findByTenantIdAndName(UUID tenantId, String name) {
        return DaoUtil.getData(notificationRuleRepository.findByTenantIdAndName(tenantId, name));
    }

    @Override
    public PageData<NotificationRule> findByTenantId(UUID tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(notificationRuleRepository.findByTenantId(tenantId, DaoUtil.toPageable(pageLink)));
    }

    @Override
    public NotificationRuleId getExternalIdByInternal(NotificationRuleId internalId) {
        return DaoUtil.toEntityId(notificationRuleRepository.getExternalIdByInternal(internalId.getId()), NotificationRuleId::new);
    }

    @Override
    public PageData<NotificationRule> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return findByTenantId(tenantId.getId(), pageLink);
    }

    @Override
    protected Class<NotificationRuleEntity> getEntityClass() {
        return NotificationRuleEntity.class;
    }

    @Override
    protected JpaRepository<NotificationRuleEntity, UUID> getRepository() {
        return notificationRuleRepository;
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.NOTIFICATION_RULE;
    }

}
