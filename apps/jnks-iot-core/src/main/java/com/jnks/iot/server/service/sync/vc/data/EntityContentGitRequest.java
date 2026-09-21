package com.jnks.iot.server.service.sync.vc.data;

import lombok.Getter;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;

@Getter
public class EntityContentGitRequest extends PendingGitRequest<EntityExportData> {

    private final String versionId;
    private final EntityId entityId;

    public EntityContentGitRequest(TenantId tenantId, String versionId, EntityId entityId) {
        super(tenantId);
        this.versionId = versionId;
        this.entityId = entityId;
    }
}
