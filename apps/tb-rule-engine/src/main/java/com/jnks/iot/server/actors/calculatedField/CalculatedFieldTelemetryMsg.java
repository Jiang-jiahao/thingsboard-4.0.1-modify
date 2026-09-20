package com.jnks.iot.server.actors.calculatedField;

import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;
import com.jnks.iot.server.common.msg.queue.TbCallback;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldTelemetryMsgProto;

@Data
public class CalculatedFieldTelemetryMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final EntityId entityId;
    private final CalculatedFieldTelemetryMsgProto proto;
    private final TbCallback callback;


    @Override
    public MsgType getMsgType() {
        return MsgType.CF_TELEMETRY_MSG;
    }
}
