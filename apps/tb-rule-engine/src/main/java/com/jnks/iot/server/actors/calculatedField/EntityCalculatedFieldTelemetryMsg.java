package com.jnks.iot.server.actors.calculatedField;

import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;
import com.jnks.iot.server.common.msg.queue.TbCallback;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldTelemetryMsgProto;
import com.jnks.iot.server.service.cf.ctx.state.CalculatedFieldCtx;

import java.util.List;

@Data
public class EntityCalculatedFieldTelemetryMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final EntityId entityId;
    private final CalculatedFieldTelemetryMsgProto proto;
    // Both lists are effectively immutable in CalculatedFieldManagerMessageProcessor and must stay so.
    private final List<CalculatedFieldCtx> entityIdFields;
    private final List<CalculatedFieldCtx> profileIdFields;
    private final TbCallback callback;

    public EntityCalculatedFieldTelemetryMsg(CalculatedFieldTelemetryMsg msg,
                                             List<CalculatedFieldCtx> entityIdFields,
                                             List<CalculatedFieldCtx> profileIdFields,
                                             TbCallback callback) {
        this.tenantId = msg.getTenantId();
        this.entityId = msg.getEntityId();
        this.proto = msg.getProto();
        this.entityIdFields = entityIdFields;
        this.profileIdFields = profileIdFields;
        this.callback = callback;
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_ENTITY_TELEMETRY_MSG;
    }
}
