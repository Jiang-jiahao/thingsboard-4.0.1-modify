package com.jnks.iot.server.service.sync.vc.data;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.vc.BranchInfo;

import java.util.List;

public class ListBranchesGitRequest extends PendingGitRequest<List<BranchInfo>> {

    public ListBranchesGitRequest(TenantId tenantId) {
        super(tenantId);
    }

}
