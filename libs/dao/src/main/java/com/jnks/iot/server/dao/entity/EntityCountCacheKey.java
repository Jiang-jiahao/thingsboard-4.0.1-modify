package com.jnks.iot.server.dao.entity;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;

import java.io.Serial;
import java.io.Serializable;

@Getter
@EqualsAndHashCode
@RequiredArgsConstructor
public class EntityCountCacheKey implements Serializable {

    @Serial
    private static final long serialVersionUID = -1992105662738434178L;

    private final TenantId tenantId;
    private final EntityType entityType;

    @Override
    public String toString() {
        return tenantId + "_" + entityType;
    }

}
