package org.thingsboard.server.service.notification;

import org.thingsboard.server.common.data.id.NotificationRequestId;
import org.thingsboard.server.common.data.id.TenantId;

/**
 * 通知请求调度接口：按延迟时间安排通知发送。
 */
public interface NotificationSchedulerService {

    /** 按请求创建时间与延迟秒数调度发送。 */
    void scheduleNotificationRequest(TenantId tenantId, NotificationRequestId notificationRequestId, long requestTs);

}
