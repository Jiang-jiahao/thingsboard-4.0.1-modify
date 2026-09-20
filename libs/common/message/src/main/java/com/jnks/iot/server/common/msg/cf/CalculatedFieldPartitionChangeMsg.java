package com.jnks.iot.server.common.msg.cf;

import lombok.Data;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;

import java.util.Set;

@Data
public class CalculatedFieldPartitionChangeMsg implements ToCalculatedFieldSystemMsg {

    @Override
    public TenantId getTenantId() {
        return TenantId.SYS_TENANT_ID;
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_PARTITIONS_CHANGE_MSG;
    }
}
