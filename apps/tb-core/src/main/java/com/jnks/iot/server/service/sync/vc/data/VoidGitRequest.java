package com.jnks.iot.server.service.sync.vc.data;

import com.jnks.iot.server.common.data.id.TenantId;

public class VoidGitRequest extends PendingGitRequest<Void> {

    public VoidGitRequest(TenantId tenantId) {
        super(tenantId);
    }

}
