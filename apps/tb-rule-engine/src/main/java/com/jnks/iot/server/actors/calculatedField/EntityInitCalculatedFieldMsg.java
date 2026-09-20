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
public class EntityInitCalculatedFieldMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final CalculatedFieldCtx ctx;
    private final TbCallback callback;
    private final boolean forceReinit;

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_ENTITY_INIT_CF_MSG;
    }
}
