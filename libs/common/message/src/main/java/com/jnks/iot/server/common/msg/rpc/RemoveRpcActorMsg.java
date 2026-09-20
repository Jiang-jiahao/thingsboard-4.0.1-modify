package com.jnks.iot.server.common.msg.rpc;

import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToDeviceActorNotificationMsg;

import java.util.UUID;

@Data
public class RemoveRpcActorMsg implements ToDeviceActorNotificationMsg {

    private static final long serialVersionUID = -6112720854949677477L;

    private final TenantId tenantId;
    private final DeviceId deviceId;
    private final UUID requestId;

    @Override
    public MsgType getMsgType() {
        return MsgType.REMOVE_RPC_TO_DEVICE_ACTOR_MSG;
    }
}
