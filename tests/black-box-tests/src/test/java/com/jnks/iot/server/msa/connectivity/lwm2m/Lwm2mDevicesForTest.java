package com.jnks.iot.server.msa.connectivity.lwm2m;


import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.msa.connectivity.lwm2m.client.LwM2MTestClient;

@Slf4j
@Data
public class Lwm2mDevicesForTest {

    Device lwM2MDeviceTest;
    LwM2MTestClient lwM2MTestClient;
    DeviceProfile lwm2mDeviceProfile;
    public Lwm2mDevicesForTest(DeviceProfile deviceProfile) {
        this.lwm2mDeviceProfile = deviceProfile;
    }
}
