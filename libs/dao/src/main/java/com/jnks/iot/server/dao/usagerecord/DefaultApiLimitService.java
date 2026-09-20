package com.jnks.iot.server.dao.usagerecord;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.query.EntityCountQuery;
import com.jnks.iot.server.common.data.query.EntityTypeFilter;
import com.jnks.iot.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import com.jnks.iot.server.dao.entity.EntityService;
import com.jnks.iot.server.dao.tenant.TbTenantProfileCache;

import java.util.function.Function;

@Service
@RequiredArgsConstructor
public class DefaultApiLimitService implements ApiLimitService {

    private final EntityService entityService;
    private final TbTenantProfileCache tenantProfileCache;

    @Override
    public boolean checkEntitiesLimit(TenantId tenantId, EntityType entityType) {
        long limit = getLimit(tenantId, profileConfiguration -> profileConfiguration.getEntitiesLimit(entityType));
        if (limit <= 0) {
            return true;
        }

        EntityTypeFilter filter = new EntityTypeFilter();
        filter.setEntityType(entityType);
        long currentCount = entityService.countEntitiesByQuery(tenantId, new CustomerId(EntityId.NULL_UUID), new EntityCountQuery(filter));
        return currentCount < limit;
    }

    @Override
    public long getLimit(TenantId tenantId, Function<DefaultTenantProfileConfiguration, Number> extractor) {
        if (tenantId == null || tenantId.isSysTenantId()) {
            return 0L;
        }
        TenantProfile tenantProfile = tenantProfileCache.get(tenantId);
        if (tenantProfile == null) {
            throw new IllegalArgumentException("Tenant profile not found for tenant " + tenantId);
        }
        Number value = extractor.apply(tenantProfile.getDefaultProfileConfiguration());
        if (value == null) {
            return 0L;
        }
        return Math.max(0, value.longValue());
    }

}
