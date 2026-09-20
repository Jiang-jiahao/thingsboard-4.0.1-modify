package com.jnks.iot.server.dao.usage;

import com.jnks.iot.server.common.data.UsageInfo;
import com.jnks.iot.server.common.data.id.TenantId;

public interface UsageInfoService {

    UsageInfo getUsageInfo(TenantId tenantId);

}
