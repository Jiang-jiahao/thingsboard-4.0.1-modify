package com.jnks.iot.server.service.cf.ctx;

import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;

public record CalculatedFieldEntityCtxId(TenantId tenantId, CalculatedFieldId cfId, EntityId entityId) {

    public String toKey() {
        return cfId + "_" + entityId;
    }

}
