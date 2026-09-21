package com.jnks.iot.server.service.sync.vc.data;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.vc.VersionedEntityInfo;

import java.util.List;

public class ListEntitiesGitRequest extends PendingGitRequest<List<VersionedEntityInfo>> {

    public ListEntitiesGitRequest(TenantId tenantId) {
        super(tenantId);
    }

}
