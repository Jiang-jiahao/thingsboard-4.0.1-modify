package org.thingsboard.server.common.msg.cf;

import lombok.Data;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.MsgType;
import org.thingsboard.server.common.msg.ToCalculatedFieldSystemMsg;

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
