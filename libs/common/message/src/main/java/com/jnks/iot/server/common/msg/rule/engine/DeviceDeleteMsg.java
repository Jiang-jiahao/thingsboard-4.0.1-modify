package com.jnks.iot.server.common.msg.rule.engine;

import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToDeviceActorNotificationMsg;

@Data
public class DeviceDeleteMsg implements ToDeviceActorNotificationMsg {

    private static final long serialVersionUID = 4679029228395462172L;

    private final TenantId tenantId;
    private final DeviceId deviceId;

    @Override
    public MsgType getMsgType() {
        return MsgType.DEVICE_DELETE_TO_DEVICE_ACTOR_MSG;
    }
}
