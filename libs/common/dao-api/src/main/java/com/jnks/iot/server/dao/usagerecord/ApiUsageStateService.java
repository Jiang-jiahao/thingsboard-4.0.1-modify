package com.jnks.iot.server.dao.usagerecord;

import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.id.ApiUsageStateId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.entity.EntityDaoService;

public interface ApiUsageStateService extends EntityDaoService {

    ApiUsageState createDefaultApiUsageState(TenantId id, EntityId entityId);

    ApiUsageState update(ApiUsageState apiUsageState);

    ApiUsageState findTenantApiUsageState(TenantId tenantId);

    ApiUsageState findApiUsageStateByEntityId(EntityId entityId);

    void deleteApiUsageStateByEntityId(EntityId entityId);

    ApiUsageState findApiUsageStateById(TenantId tenantId, ApiUsageStateId id);

}
