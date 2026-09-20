package com.jnks.iot.server.dao.notification;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.NotificationRequestId;
import com.jnks.iot.server.common.data.id.NotificationRuleId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.NotificationRequest;
import com.jnks.iot.server.common.data.notification.NotificationRequestInfo;
import com.jnks.iot.server.common.data.notification.NotificationRequestStats;
import com.jnks.iot.server.common.data.notification.NotificationRequestStatus;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;

import java.util.List;

public interface NotificationRequestService {

    NotificationRequest saveNotificationRequest(TenantId tenantId, NotificationRequest notificationRequest);

    NotificationRequest findNotificationRequestById(TenantId tenantId, NotificationRequestId id);

    NotificationRequestInfo findNotificationRequestInfoById(TenantId tenantId, NotificationRequestId id);

    PageData<NotificationRequest> findNotificationRequestsByTenantIdAndOriginatorType(TenantId tenantId, EntityType originatorType, PageLink pageLink);

    PageData<NotificationRequestInfo> findNotificationRequestsInfosByTenantIdAndOriginatorType(TenantId tenantId, EntityType originatorType, PageLink pageLink);

    List<NotificationRequestId> findNotificationRequestsIdsByStatusAndRuleId(TenantId tenantId, NotificationRequestStatus requestStatus, NotificationRuleId ruleId);

    List<NotificationRequest> findNotificationRequestsByRuleIdAndOriginatorEntityIdAndStatus(TenantId tenantId, NotificationRuleId ruleId, EntityId originatorEntityId, NotificationRequestStatus status);

    void deleteNotificationRequest(TenantId tenantId, NotificationRequest request);

    PageData<NotificationRequest> findScheduledNotificationRequests(PageLink pageLink);

    void updateNotificationRequest(TenantId tenantId, NotificationRequestId requestId, NotificationRequestStatus requestStatus, NotificationRequestStats stats);

    void deleteNotificationRequestsByTenantId(TenantId tenantId);

}
