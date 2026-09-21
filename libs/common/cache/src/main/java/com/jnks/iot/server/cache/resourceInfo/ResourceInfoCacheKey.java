package com.jnks.iot.server.cache.resourceInfo;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.id.JnksIotResourceId;
import com.jnks.iot.server.common.data.id.TenantId;

import java.io.Serial;
import java.io.Serializable;

@Getter
@EqualsAndHashCode
@RequiredArgsConstructor
@Builder
public class ResourceInfoCacheKey implements Serializable {

    @Serial
    private static final long serialVersionUID = 2100510964692846992L;

    private final TenantId tenantId;
    private final JnksIotResourceId jnksIotResourceId;

    @Override
    public String toString() {
        return tenantId + "_" + jnksIotResourceId;
    }

}
