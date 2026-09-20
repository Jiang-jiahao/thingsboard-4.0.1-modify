package com.jnks.iot.server.dao.usagerecord;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;

import java.util.function.Function;

public interface ApiLimitService {

    boolean checkEntitiesLimit(TenantId tenantId, EntityType entityType);

    long getLimit(TenantId tenantId, Function<DefaultTenantProfileConfiguration, Number> extractor);

}
