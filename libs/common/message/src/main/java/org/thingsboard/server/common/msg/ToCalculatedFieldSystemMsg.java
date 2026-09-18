package org.thingsboard.server.common.msg;

import org.thingsboard.server.common.msg.aware.TenantAwareMsg;
import org.thingsboard.server.common.msg.queue.TbCallback;

public interface ToCalculatedFieldSystemMsg extends TenantAwareMsg {

    default TbCallback getCallback() {
        return TbCallback.EMPTY;
    }

}
