package com.jnks.iot.server.actors.calculatedField;

import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;

@Data
public class CalculatedFieldEntityDeleteMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final EntityId entityId;
    private final JnksIotCallback callback;

    public CalculatedFieldEntityDeleteMsg(TenantId tenantId,
                                          EntityId entityId,
                                          JnksIotCallback callback) {
        this.tenantId = tenantId;
        this.entityId = entityId;
        this.callback = callback;
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_ENTITY_DELETE_MSG;
    }
}
