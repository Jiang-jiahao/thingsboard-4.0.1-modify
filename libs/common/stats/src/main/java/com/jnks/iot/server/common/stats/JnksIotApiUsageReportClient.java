package com.jnks.iot.server.common.stats;

import com.jnks.iot.server.common.data.ApiUsageRecordKey;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;

public interface JnksIotApiUsageReportClient {

    void report(TenantId tenantId, CustomerId customerId, ApiUsageRecordKey key, long value);

    void report(TenantId tenantId, CustomerId customerId, ApiUsageRecordKey key);

}
