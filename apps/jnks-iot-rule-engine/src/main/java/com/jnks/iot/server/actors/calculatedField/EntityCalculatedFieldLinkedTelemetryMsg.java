package com.jnks.iot.server.actors.calculatedField;

import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldTelemetryMsgProto;
import com.jnks.iot.server.service.cf.ctx.state.CalculatedFieldCtx;

import java.util.List;

@Data
public class EntityCalculatedFieldLinkedTelemetryMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final EntityId entityId;
    private final CalculatedFieldTelemetryMsgProto proto;
    private final CalculatedFieldCtx ctx;
    private final JnksIotCallback callback;

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_LINKED_TELEMETRY_MSG;
    }
}
