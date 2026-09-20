package com.jnks.iot.rule.engine.api;

import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.id.ApiUsageStateId;
import com.jnks.iot.server.common.data.id.TenantId;

public interface RuleEngineApiUsageStateService {

    ApiUsageState findApiUsageStateById(TenantId tenantId, ApiUsageStateId id);

}
