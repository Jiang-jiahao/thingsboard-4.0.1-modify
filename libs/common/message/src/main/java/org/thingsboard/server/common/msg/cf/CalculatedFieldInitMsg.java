package org.thingsboard.server.common.msg.cf;

import lombok.Data;
import org.thingsboard.server.common.data.cf.CalculatedField;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.MsgType;
import org.thingsboard.server.common.msg.ToCalculatedFieldSystemMsg;

@Data
public class CalculatedFieldInitMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final CalculatedField cf;

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_INIT_MSG;
    }
}
