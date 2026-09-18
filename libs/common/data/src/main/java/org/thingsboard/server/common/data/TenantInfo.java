package org.thingsboard.server.common.data;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import org.thingsboard.server.common.data.id.TenantId;

@Schema
@Data
public class TenantInfo extends Tenant {
    @Schema(description = "Tenant Profile name", example = "Default")
    private String tenantProfileName;

    public TenantInfo() {
        super();
    }

    public TenantInfo(TenantId tenantId) {
        super(tenantId);
    }

    public TenantInfo(Tenant tenant, String tenantProfileName) {
        super(tenant);
        this.tenantProfileName = tenantProfileName;
    }

}
