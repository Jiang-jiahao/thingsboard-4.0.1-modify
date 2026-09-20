package com.jnks.iot.server.common.msg;

import com.jnks.iot.server.common.msg.aware.TenantAwareMsg;
import com.jnks.iot.server.common.msg.queue.TbCallback;

public interface ToCalculatedFieldSystemMsg extends TenantAwareMsg {

    default TbCallback getCallback() {
        return TbCallback.EMPTY;
    }

}
