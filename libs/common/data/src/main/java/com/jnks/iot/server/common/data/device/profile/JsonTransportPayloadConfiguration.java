package com.jnks.iot.server.common.data.device.profile;

import lombok.Data;
import com.jnks.iot.server.common.data.TransportPayloadType;

@Data
public class JsonTransportPayloadConfiguration implements TransportPayloadTypeConfiguration {

    @Override
    public TransportPayloadType getTransportPayloadType() {
        return TransportPayloadType.JSON;
    }
}
