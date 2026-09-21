package com.jnks.iot.server.transport.lwm2m.secure;

import lombok.Data;
import com.jnks.iot.server.common.transport.auth.ValidateDeviceCredentialsResponse;

import java.io.Serializable;

@Data
public class JnksIotX509DtlsSessionInfo implements Serializable {

    private final String x509CommonName;
    private final ValidateDeviceCredentialsResponse credentials;

}
