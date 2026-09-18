package org.thingsboard.server.dao.mobile;

import lombok.Data;
import org.thingsboard.server.common.data.id.TenantId;

@Data
public class QrCodeSettingsEvictEvent {
    private final TenantId tenantId;
}
