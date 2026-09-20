package com.jnks.iot.server.common.data.device.profile;

import lombok.Data;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.device.data.PowerSavingConfiguration;

@Data
public class CoapDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    private CoapDeviceTypeConfiguration coapDeviceTypeConfiguration;
    private PowerSavingConfiguration clientSettings;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.COAP;
    }

    public CoapDeviceTypeConfiguration getCoapDeviceTypeConfiguration() {
        if (coapDeviceTypeConfiguration != null) {
            return coapDeviceTypeConfiguration;
        } else {
            return new DefaultCoapDeviceTypeConfiguration();
        }
    }
}