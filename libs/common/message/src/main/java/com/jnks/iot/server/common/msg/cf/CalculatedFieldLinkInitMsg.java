package com.jnks.iot.server.common.msg.cf;

import lombok.Data;
import com.jnks.iot.server.common.data.cf.CalculatedFieldLink;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;

@Data
public class CalculatedFieldLinkInitMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final CalculatedFieldLink link;

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_LINK_INIT_MSG;
    }
}
