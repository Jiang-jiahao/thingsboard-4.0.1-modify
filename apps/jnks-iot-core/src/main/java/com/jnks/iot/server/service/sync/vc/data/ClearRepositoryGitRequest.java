package com.jnks.iot.server.service.sync.vc.data;

import com.jnks.iot.server.common.data.id.TenantId;

public class ClearRepositoryGitRequest extends VoidGitRequest {

    public ClearRepositoryGitRequest(TenantId tenantId) {
        super(tenantId);
    }

    public boolean requiresSettings() {
        return false;
    }

}
