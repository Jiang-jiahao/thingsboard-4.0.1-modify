package com.jnks.iot.server.service.sync.vc.data;

import lombok.Getter;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.vc.EntityVersionsDiff;

import java.util.List;

@Getter
public class VersionsDiffGitRequest extends PendingGitRequest<List<EntityVersionsDiff>> {

    private final String path;
    private final String versionId1;
    private final String versionId2;

    public VersionsDiffGitRequest(TenantId tenantId, String path, String versionId1, String versionId2) {
        super(tenantId);
        this.path = path;
        this.versionId1 = versionId1;
        this.versionId2 = versionId2;
    }

}
