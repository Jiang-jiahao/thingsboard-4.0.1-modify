package com.jnks.iot.server.dao.notification;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.HasId;
import com.jnks.iot.server.common.data.id.NotificationRequestId;
import com.jnks.iot.server.common.data.id.NotificationRuleId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationRequest;
import com.jnks.iot.server.common.data.notification.NotificationRequestInfo;
import com.jnks.iot.server.common.data.notification.NotificationRequestStats;
import com.jnks.iot.server.common.data.notification.NotificationRequestStatus;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.entity.EntityDaoService;
import com.jnks.iot.server.dao.eventsourcing.DeleteEntityEvent;
import com.jnks.iot.server.dao.service.DataValidator;

import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class DefaultNotificationRequestService implements NotificationRequestService, EntityDaoService {

    private final NotificationRequestDao notificationRequestDao;
    private final NotificationDao notificationDao;

    private final ApplicationEventPublisher eventPublisher;

    private final NotificationRequestValidator notificationRequestValidator = new NotificationRequestValidator();

    @Override
    public NotificationRequest saveNotificationRequest(TenantId tenantId, NotificationRequest notificationRequest) {
        notificationRequestValidator.validate(notificationRequest, NotificationRequest::getTenantId);
        return notificationRequestDao.save(tenantId, notificationRequest);
    }

    @Override
    public NotificationRequest findNotificationRequestById(TenantId tenantId, NotificationRequestId id) {
        return notificationRequestDao.findById(tenantId, id.getId());
    }

    @Override
    public NotificationRequestInfo findNotificationRequestInfoById(TenantId tenantId, NotificationRequestId id) {
        return notificationRequestDao.findInfoById(tenantId, id);
    }

    @Override
    public PageData<NotificationRequest> findNotificationRequestsByTenantIdAndOriginatorType(TenantId tenantId, EntityType originatorType, PageLink pageLink) {
        return notificationRequestDao.findByTenantIdAndOriginatorTypeAndPageLink(tenantId, originatorType, pageLink);
    }

    @Override
    public PageData<NotificationRequestInfo> findNotificationRequestsInfosByTenantIdAndOriginatorType(TenantId tenantId, EntityType originatorType, PageLink pageLink) {
        return notificationRequestDao.findInfosByTenantIdAndOriginatorTypeAndPageLink(tenantId, originatorType, pageLink);
    }

    @Override
    public List<NotificationRequestId> findNotificationRequestsIdsByStatusAndRuleId(TenantId tenantId, NotificationRequestStatus requestStatus, NotificationRuleId ruleId) {
        return notificationRequestDao.findIdsByRuleId(tenantId, requestStatus, ruleId);
    }

    @Override
    public List<NotificationRequest> findNotificationRequestsByRuleIdAndOriginatorEntityIdAndStatus(TenantId tenantId, NotificationRuleId ruleId, EntityId originatorEntityId, NotificationRequestStatus status) {
        return notificationRequestDao.findByRuleIdAndOriginatorEntityIdAndStatus(tenantId, ruleId, originatorEntityId, status);
    }

    @Override
    public void deleteNotificationRequest(TenantId tenantId, NotificationRequest request) {
        notificationRequestDao.removeById(tenantId, request.getUuidId());
        notificationDao.deleteByRequestId(tenantId, request.getId());
        eventPublisher.publishEvent(DeleteEntityEvent.builder().tenantId(tenantId).entity(request).entityId(request.getId()).build());
    }

    @Override
    public void deleteEntity(TenantId tenantId, EntityId id, boolean force) {
        if (force) {
            notificationRequestDao.removeById(tenantId, id.getId());
        } else {
            NotificationRequest notificationRequest = findNotificationRequestById(tenantId, (NotificationRequestId) id);
            deleteNotificationRequest(tenantId, notificationRequest);
        }
    }

    @Override
    public PageData<NotificationRequest> findScheduledNotificationRequests(PageLink pageLink) {
        return notificationRequestDao.findAllByStatus(NotificationRequestStatus.SCHEDULED, pageLink);
    }

    @Override
    public void updateNotificationRequest(TenantId tenantId, NotificationRequestId requestId, NotificationRequestStatus requestStatus, NotificationRequestStats stats) {
        notificationRequestDao.updateById(tenantId, requestId, requestStatus, stats);
    }

    // notifications themselves are left in the database until removed by ttl
    @Override
    public void deleteNotificationRequestsByTenantId(TenantId tenantId) {
        notificationRequestDao.removeByTenantId(tenantId);
    }

    @Override
    public void deleteByTenantId(TenantId tenantId) {
        deleteNotificationRequestsByTenantId(tenantId);
    }

    @Override
    public Optional<HasId<?>> findEntity(TenantId tenantId, EntityId entityId) {
        return Optional.ofNullable(findNotificationRequestById(tenantId, new NotificationRequestId(entityId.getId())));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.NOTIFICATION_REQUEST;
    }

    private static class NotificationRequestValidator extends DataValidator<NotificationRequest> {

    }

}
