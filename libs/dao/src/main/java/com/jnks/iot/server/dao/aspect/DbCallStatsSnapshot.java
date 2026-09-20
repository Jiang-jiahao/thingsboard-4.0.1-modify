package com.jnks.iot.server.dao.aspect;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.Map;

@Data
@Builder
public class DbCallStatsSnapshot {

    private final TenantId tenantId;
    private final int totalSuccess;
    private final int totalFailure;
    private final long totalTiming;
    private final Map<String, MethodCallStatsSnapshot> methodStats;

    public int getTotalCalls() {
        return totalSuccess + totalFailure;
    }

}
