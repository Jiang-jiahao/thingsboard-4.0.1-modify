package com.jnks.iot.server.common.data;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.Set;

@Data
@Builder
public class TbResourceInfoFilter {

    private TenantId tenantId;
    private Set<ResourceType> resourceTypes;
    private Set<ResourceSubType> resourceSubTypes;

}
