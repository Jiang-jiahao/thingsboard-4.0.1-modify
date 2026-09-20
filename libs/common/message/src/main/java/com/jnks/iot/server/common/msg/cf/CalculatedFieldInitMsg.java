package com.jnks.iot.server.common.msg.cf;

import lombok.Data;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;

@Data
public class CalculatedFieldInitMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final CalculatedField cf;

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_INIT_MSG;
    }
}
