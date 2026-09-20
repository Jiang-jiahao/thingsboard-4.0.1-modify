package com.jnks.iot.server.common.msg.cf;

import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;

@Data
public class CalculatedFieldCacheInitMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_CACHE_INIT_MSG;
    }

}
