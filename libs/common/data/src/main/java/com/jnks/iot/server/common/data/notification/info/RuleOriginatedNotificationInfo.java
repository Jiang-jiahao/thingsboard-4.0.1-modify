package com.jnks.iot.server.common.data.notification.info;

import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;

public interface RuleOriginatedNotificationInfo extends NotificationInfo {

    default CustomerId getAffectedCustomerId() {
        return null;
    }

    default UserId getAffectedUserId() {
        return null;
    }

    default TenantId getAffectedTenantId() {
        return null;
    }

}
