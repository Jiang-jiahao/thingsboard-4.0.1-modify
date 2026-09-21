package com.jnks.iot.server.common.stats;

import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.id.TenantId;

public interface JnksIotApiUsageStateClient {

    ApiUsageState getApiUsageState(TenantId tenantId);

}
