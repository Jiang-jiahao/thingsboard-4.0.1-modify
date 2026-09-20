package com.jnks.iot.server.dao.device;

import lombok.Data;

@Data
class DeviceCredentialsEvictEvent {

    private final String newCedentialsId;
    private final String oldCredentialsId;

}
