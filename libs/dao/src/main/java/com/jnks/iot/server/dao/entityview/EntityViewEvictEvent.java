package com.jnks.iot.server.dao.entityview;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityViewId;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
class EntityViewEvictEvent {

    private final TenantId tenantId;
    private final EntityViewId entityViewId;
    private final EntityId newEntityId;
    private final EntityId oldEntityId;
    private final String newName;
    private final String oldName;
    private EntityView savedEntityView;

}
