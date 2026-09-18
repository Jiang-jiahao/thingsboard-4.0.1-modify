package org.thingsboard.server.common.msg.cf;

import lombok.Data;
import org.thingsboard.server.common.data.cf.CalculatedFieldLink;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.MsgType;
import org.thingsboard.server.common.msg.ToCalculatedFieldSystemMsg;

@Data
public class CalculatedFieldLinkInitMsg implements ToCalculatedFieldSystemMsg {

    private final TenantId tenantId;
    private final CalculatedFieldLink link;

    @Override
    public MsgType getMsgType() {
        return MsgType.CF_LINK_INIT_MSG;
    }
}
