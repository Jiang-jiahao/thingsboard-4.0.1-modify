package com.jnks.iot.server.dao.device;

import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.dao.device.provision.ProvisionFailedException;
import com.jnks.iot.server.dao.device.provision.ProvisionRequest;
import com.jnks.iot.server.dao.device.provision.ProvisionResponse;

public interface DeviceProvisionService {

    ProvisionResponse provisionDevice(ProvisionRequest provisionRequest) throws ProvisionFailedException;

    ProvisionResponse provisionDeviceViaX509Chain(DeviceProfile deviceProfile, ProvisionRequest provisionRequest) throws ProvisionFailedException;
}
