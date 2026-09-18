package org.thingsboard.server.common.data;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.id.TenantId;

import java.util.Set;

@Data
@Builder
public class TbResourceInfoFilter {

    private TenantId tenantId;
    private Set<ResourceType> resourceTypes;
    private Set<ResourceSubType> resourceSubTypes;

}
