package com.jnks.iot.server.transport.lwm2m.secure;

import lombok.Data;
import org.eclipse.leshan.core.SecurityMode;
import org.eclipse.leshan.server.bootstrap.BootstrapConfig;
import org.eclipse.leshan.server.security.SecurityInfo;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import com.jnks.iot.server.transport.lwm2m.bootstrap.secure.LwM2MBootstrapConfig;

import java.io.Serializable;

@Data
public class TbLwM2MSecurityInfo implements Serializable {
    private ValidateDeviceCredentialsResponse msg;
    private DeviceProfile deviceProfile;
    private String endpoint;
    private SecurityInfo securityInfo;
    private SecurityMode securityMode;


    /** bootstrap */
    private LwM2MBootstrapConfig bootstrapCredentialConfig;
    private BootstrapConfig bootstrapConfig;
}
