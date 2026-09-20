package com.jnks.iot.server.cache.user;

import com.jnks.iot.server.common.data.id.TenantId;

public record UserCacheEvictEvent(TenantId tenantId, String newEmail, String oldEmail) {
}
