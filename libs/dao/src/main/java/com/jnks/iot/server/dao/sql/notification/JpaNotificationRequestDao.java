package com.jnks.iot.server.dao.sql.notification;

import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.NotificationRequestId;
import com.jnks.iot.server.common.data.id.NotificationRuleId;
import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.id.NotificationTemplateId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationRequest;
import com.jnks.iot.server.common.data.notification.NotificationRequestInfo;
import com.jnks.iot.server.common.data.notification.NotificationRequestStats;
import com.jnks.iot.server.common.data.notification.NotificationRequestStatus;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.model.sql.NotificationRequestEntity;
import com.jnks.iot.server.dao.model.sql.NotificationRequestInfoEntity;
import com.jnks.iot.server.dao.notification.NotificationRequestDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
@SqlDao
@RequiredArgsConstructor
public class JpaNotificationRequestDao extends JpaAbstractDao<NotificationRequestEntity, NotificationRequest> implements NotificationRequestDao {

    private final NotificationRequestRepository notificationRequestRepository;

    @Override
    public PageData<NotificationRequest> findByTenantIdAndOriginatorTypeAndPageLink(TenantId tenantId, EntityType originatorType, PageLink pageLink) {
        return DaoUtil.toPageData(notificationRequestRepository.findByTenantIdAndOriginatorEntityType(tenantId.getId(),
                originatorType, DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<NotificationRequestInfo> findInfosByTenantIdAndOriginatorTypeAndPageLink(TenantId tenantId, EntityType originatorType, PageLink pageLink) {
        return DaoUtil.pageToPageData(notificationRequestRepository.findInfosByTenantIdAndOriginatorEntityTypeAndSearchText(tenantId.getId(),
                        originatorType, pageLink.getTextSearch(), DaoUtil.toPageable(pageLink, Map.of(
                                "templateName", "t.name"
                        ))))
                .mapData(NotificationRequestInfoEntity::toData);
    }

    @Override
    public List<NotificationRequestId> findIdsByRuleId(TenantId tenantId, NotificationRequestStatus requestStatus, NotificationRuleId ruleId) {
        return notificationRequestRepository.findAllIdsByStatusAndRuleId(requestStatus, ruleId.getId()).stream()
                .map(NotificationRequestId::new).collect(Collectors.toList());
    }

    @Override
    public List<NotificationRequest> findByRuleIdAndOriginatorEntityIdAndStatus(TenantId tenantId, NotificationRuleId ruleId, EntityId originatorEntityId, NotificationRequestStatus status) {
        return DaoUtil.convertDataList(notificationRequestRepository.findAllByRuleIdAndOriginatorEntityIdAndOriginatorEntityTypeAndStatus(ruleId.getId(), originatorEntityId.getId(), originatorEntityId.getEntityType(), status));
    }

    @Override
    public PageData<NotificationRequest> findAllByStatus(NotificationRequestStatus status, PageLink pageLink) {
        return DaoUtil.toPageData(notificationRequestRepository.findAllByStatus(status, DaoUtil.toPageable(pageLink)));
    }

    @Override
    public void updateById(TenantId tenantId, NotificationRequestId requestId, NotificationRequestStatus requestStatus, NotificationRequestStats stats) {
        notificationRequestRepository.updateStatusAndStatsById(requestId.getId(), requestStatus, JacksonUtil.valueToTree(stats));
    }

    @Override
    public boolean existsByTenantIdAndStatusAndTargetId(TenantId tenantId, NotificationRequestStatus status, NotificationTargetId targetId) {
        return notificationRequestRepository.existsByTenantIdAndStatusAndTargetsContaining(tenantId.getId(), status, targetId.getId().toString());
    }

    @Override
    public boolean existsByTenantIdAndStatusAndTemplateId(TenantId tenantId, NotificationRequestStatus status, NotificationTemplateId templateId) {
        return notificationRequestRepository.existsByTenantIdAndStatusAndTemplateId(tenantId.getId(), status, templateId.getId());
    }

    @Override
    public int removeAllByCreatedTimeBefore(long ts) {
        return notificationRequestRepository.deleteAllByCreatedTimeBefore(ts);
    }

    @Override
    public NotificationRequestInfo findInfoById(TenantId tenantId, NotificationRequestId id) {
        NotificationRequestInfoEntity info = notificationRequestRepository.findInfoById(id.getId());
        return info != null ? info.toData() : null;
    }

    @Override
    public void removeByTenantId(TenantId tenantId) {
        notificationRequestRepository.deleteByTenantId(tenantId.getId());
    }

    @Override
    protected Class<NotificationRequestEntity> getEntityClass() {
        return NotificationRequestEntity.class;
    }

    @Override
    protected JpaRepository<NotificationRequestEntity, UUID> getRepository() {
        return notificationRequestRepository;
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.NOTIFICATION_REQUEST;
    }

}
