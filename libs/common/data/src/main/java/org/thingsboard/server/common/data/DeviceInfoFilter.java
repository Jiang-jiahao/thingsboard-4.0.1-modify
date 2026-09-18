package org.thingsboard.server.common.data;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.TenantId;

@Data
@Builder
public class DeviceInfoFilter {

    private TenantId tenantId;
    private CustomerId customerId;
    private String type;
    private DeviceProfileId deviceProfileId;
    private Boolean active;

}
