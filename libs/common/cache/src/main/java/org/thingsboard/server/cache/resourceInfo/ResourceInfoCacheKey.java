package org.thingsboard.server.cache.resourceInfo;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.thingsboard.server.common.data.id.TbResourceId;
import org.thingsboard.server.common.data.id.TenantId;

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
    private final TbResourceId tbResourceId;

    @Override
    public String toString() {
        return tenantId + "_" + tbResourceId;
    }

}
