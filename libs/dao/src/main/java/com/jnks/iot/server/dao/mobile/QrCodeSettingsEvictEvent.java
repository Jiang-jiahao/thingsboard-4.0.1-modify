package com.jnks.iot.server.dao.mobile;

import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
public class QrCodeSettingsEvictEvent {
    private final TenantId tenantId;
}
