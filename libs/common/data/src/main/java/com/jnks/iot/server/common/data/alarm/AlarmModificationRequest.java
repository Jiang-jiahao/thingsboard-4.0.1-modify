package com.jnks.iot.server.common.data.alarm;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;

public interface AlarmModificationRequest {

    TenantId getTenantId();

    AlarmSeverity getSeverity();

    long getStartTs();

    long getEndTs();

    void setStartTs(long startTs);

    void setEndTs(long endTs);

    UserId getUserId();
}
