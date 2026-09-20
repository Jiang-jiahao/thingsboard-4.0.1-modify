package com.jnks.iot.server.common.msg.rpc;

import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToDeviceActorNotificationMsg;

/**
 * Created by ashvayka on 16.04.18.
 */
@Data
public class ToDeviceRpcRequestActorMsg implements ToDeviceActorNotificationMsg {

    private static final long serialVersionUID = -8592877558138716589L;

    private final String serviceId;
    private final ToDeviceRpcRequest msg;

    @Override
    public DeviceId getDeviceId() {
        return msg.getDeviceId();
    }

    @Override
    public TenantId getTenantId() {
        return msg.getTenantId();
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.DEVICE_RPC_REQUEST_TO_DEVICE_ACTOR_MSG;
    }
}
