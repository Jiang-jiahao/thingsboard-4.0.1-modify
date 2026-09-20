package com.jnks.iot.server.common.msg.rule.engine;

import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToDeviceActorNotificationMsg;

@Data
public class DeviceNameOrTypeUpdateMsg implements ToDeviceActorNotificationMsg {

    private static final long serialVersionUID = -5738949227650536685L;

    private final TenantId tenantId;
    private final DeviceId deviceId;
    private final String deviceName;
    private final String deviceType;

    @Override
    public MsgType getMsgType() {
        return MsgType.DEVICE_NAME_OR_TYPE_UPDATE_TO_DEVICE_ACTOR_MSG;
    }
}
