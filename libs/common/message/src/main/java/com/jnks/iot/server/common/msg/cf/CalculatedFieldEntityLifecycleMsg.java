package com.jnks.iot.server.common.msg.cf;

import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;
import com.jnks.iot.server.common.msg.plugin.ComponentLifecycleMsg;

@Data
public class CalculatedFieldEntityLifecycleMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final ComponentLifecycleMsg data;

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_ENTITY_LIFECYCLE_MSG;
    }
}
