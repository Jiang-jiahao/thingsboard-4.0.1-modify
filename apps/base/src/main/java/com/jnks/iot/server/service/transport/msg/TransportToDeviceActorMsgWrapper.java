package com.jnks.iot.server.service.transport.msg;

import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;
import com.jnks.iot.server.common.msg.aware.DeviceAwareMsg;
import com.jnks.iot.server.common.msg.aware.TenantAwareMsg;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.gen.transport.TransportProtos.TransportToDeviceActorMsg;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by ashvayka on 09.10.18.
 */
@Data
public class TransportToDeviceActorMsgWrapper implements JnksIotActorMsg, DeviceAwareMsg, TenantAwareMsg, Serializable {

    private static final long serialVersionUID = 7191333353202935941L;

    private final TenantId tenantId;
    private final DeviceId deviceId;
    private final TransportToDeviceActorMsg msg;
    private final JnksIotCallback callback;

    public TransportToDeviceActorMsgWrapper(TransportToDeviceActorMsg msg, JnksIotCallback callback) {
        this.msg = msg;
        this.callback = callback;
        this.tenantId = TenantId.fromUUID(new UUID(msg.getSessionInfo().getTenantIdMSB(), msg.getSessionInfo().getTenantIdLSB()));
        this.deviceId = new DeviceId(new UUID(msg.getSessionInfo().getDeviceIdMSB(), msg.getSessionInfo().getDeviceIdLSB()));
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.TRANSPORT_TO_DEVICE_ACTOR_MSG;
    }
}
