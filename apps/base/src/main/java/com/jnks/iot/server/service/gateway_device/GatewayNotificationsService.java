package com.jnks.iot.server.service.gateway_device;

import com.jnks.iot.server.common.data.Device;

public interface GatewayNotificationsService {

    void onDeviceUpdated(Device device, Device oldDevice);

    void onDeviceDeleted(Device device);
}
