package com.jnks.iot.server.common.msg.rpc;

import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToDeviceActorNotificationMsg;

@Data
public class FromDeviceRpcResponseActorMsg implements ToDeviceActorNotificationMsg {

    private static final long serialVersionUID = -6648120137236354987L;

    private final Integer requestId;
    private final TenantId tenantId;
    private final DeviceId deviceId;
    private final FromDeviceRpcResponse msg;

    @Override
    public MsgType getMsgType() {
        return MsgType.DEVICE_RPC_RESPONSE_TO_DEVICE_ACTOR_MSG;
    }
}
