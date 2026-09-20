package com.jnks.iot.server.dao.audit.sink;

import com.jnks.iot.server.common.data.audit.AuditLog;

public interface AuditLogSink {

    void logAction(AuditLog auditLogEntry);
}
