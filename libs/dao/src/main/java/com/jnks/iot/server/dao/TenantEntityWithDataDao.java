package com.jnks.iot.server.dao;

import com.jnks.iot.server.common.data.id.TenantId;

public interface TenantEntityWithDataDao {

    Long sumDataSizeByTenantId(TenantId tenantId);
}
