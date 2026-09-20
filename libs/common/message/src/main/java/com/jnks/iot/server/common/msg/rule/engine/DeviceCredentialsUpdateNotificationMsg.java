package com.jnks.iot.server.common.msg.rule.engine;

import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.security.DeviceCredentials;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToDeviceActorNotificationMsg;

/**
 * @author Andrew Shvayka
 */
@Data
public class DeviceCredentialsUpdateNotificationMsg implements ToDeviceActorNotificationMsg {

    private static final long serialVersionUID = -3956907402411126990L;

    private final TenantId tenantId;
    private final DeviceId deviceId;

    /**
     * LwM2M
     * @return
     */
    private final DeviceCredentials deviceCredentials;

    @Override
    public MsgType getMsgType() {
        return MsgType.DEVICE_CREDENTIALS_UPDATE_TO_DEVICE_ACTOR_MSG;
    }
}
