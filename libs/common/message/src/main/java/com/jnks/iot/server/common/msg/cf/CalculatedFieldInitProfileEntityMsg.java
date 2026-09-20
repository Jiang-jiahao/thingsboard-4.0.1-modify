package com.jnks.iot.server.common.msg.cf;

import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;

@Data
public class CalculatedFieldInitProfileEntityMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final EntityId profileEntityId;
    private final EntityId entityId;

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_INIT_PROFILE_ENTITY_MSG;
    }

}
