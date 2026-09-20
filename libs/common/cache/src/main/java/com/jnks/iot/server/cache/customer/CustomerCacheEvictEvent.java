package com.jnks.iot.server.cache.customer;

import com.jnks.iot.server.common.data.id.TenantId;

public record CustomerCacheEvictEvent(TenantId tenantId, String newTitle, String oldTitle) {
}
