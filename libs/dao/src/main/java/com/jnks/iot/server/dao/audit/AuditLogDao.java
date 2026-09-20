package com.jnks.iot.server.dao.audit;

import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.audit.AuditLog;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.TimePageLink;
import com.jnks.iot.server.dao.Dao;

import java.util.List;
import java.util.UUID;

public interface AuditLogDao extends Dao<AuditLog> {

    PageData<AuditLog> findAuditLogsByTenantIdAndEntityId(UUID tenantId, EntityId entityId, List<ActionType> actionTypes, TimePageLink pageLink);

    PageData<AuditLog> findAuditLogsByTenantIdAndCustomerId(UUID tenantId, CustomerId customerId, List<ActionType> actionTypes, TimePageLink pageLink);

    PageData<AuditLog> findAuditLogsByTenantIdAndUserId(UUID tenantId, UserId userId, List<ActionType> actionTypes, TimePageLink pageLink);

    PageData<AuditLog> findAuditLogsByTenantId(UUID tenantId, List<ActionType> actionTypes, TimePageLink pageLink);

    void cleanUpAuditLogs(long expTime);

}
