package org.thingsboard.server.cache.user;

import org.thingsboard.server.common.data.id.TenantId;

public record UserCacheEvictEvent(TenantId tenantId, String newEmail, String oldEmail) {
}
