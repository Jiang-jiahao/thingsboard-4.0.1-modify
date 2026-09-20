package com.jnks.iot.server.actors.calculatedField;

import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;
import com.jnks.iot.server.service.cf.ctx.CalculatedFieldEntityCtxId;
import com.jnks.iot.server.service.cf.ctx.state.CalculatedFieldState;

@Data
public class CalculatedFieldStateRestoreMsg implements ToCalculatedFieldSystemMsg {

    private final CalculatedFieldEntityCtxId id;
    private final CalculatedFieldState state;

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_STATE_RESTORE_MSG;
    }

    @Override
    public TenantId getTenantId() {
        return id.tenantId();
    }
}
