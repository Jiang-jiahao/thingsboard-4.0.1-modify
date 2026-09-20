package com.jnks.iot.server.common.transport.auth;

import lombok.Data;
import com.jnks.iot.server.common.data.device.data.PowerMode;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.TenantId;

import java.io.Serializable;

@Data
public class TransportDeviceInfo implements Serializable {

    private TenantId tenantId;
    private CustomerId customerId;
    private DeviceProfileId deviceProfileId;
    private DeviceId deviceId;
    private String deviceName;
    private String deviceType;
    private PowerMode powerMode;
    private String additionalInfo;
    private Long edrxCycle;
    private Long psmActivityTimer;
    private Long pagingTransmissionWindow;
    private boolean gateway;
}
