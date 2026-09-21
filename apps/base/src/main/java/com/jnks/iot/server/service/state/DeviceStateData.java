package com.jnks.iot.server.service.state;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

/**
 * Created by ashvayka on 01.05.18.
 */
@Data
@Builder
class DeviceStateData {

    private final TenantId tenantId;
    private final CustomerId customerId;
    private final DeviceId deviceId;
    private final long deviceCreationTime;
    private JnksIotMsgMetaData metaData;
    private final DeviceState state;
    
}
