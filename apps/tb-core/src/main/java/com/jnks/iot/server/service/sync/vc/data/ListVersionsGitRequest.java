package com.jnks.iot.server.service.sync.vc.data;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.sync.vc.EntityVersion;

public class ListVersionsGitRequest extends PendingGitRequest<PageData<EntityVersion>> {

    public ListVersionsGitRequest(TenantId tenantId) {
        super(tenantId);
    }

}
