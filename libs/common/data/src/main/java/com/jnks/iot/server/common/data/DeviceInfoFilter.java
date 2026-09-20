package com.jnks.iot.server.common.data;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
@Builder
public class DeviceInfoFilter {

    private TenantId tenantId;
    private CustomerId customerId;
    private String type;
    private DeviceProfileId deviceProfileId;
    private Boolean active;

}
