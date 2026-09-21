package com.jnks.iot.server.common.msg;

import com.jnks.iot.server.common.msg.aware.TenantAwareMsg;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;

public interface ToCalculatedFieldSystemMsg extends TenantAwareMsg {

    default JnksIotCallback getCallback() {
        return JnksIotCallback.EMPTY;
    }

}
