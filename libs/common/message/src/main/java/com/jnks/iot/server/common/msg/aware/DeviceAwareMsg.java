package com.jnks.iot.server.common.msg.aware;

import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

public interface DeviceAwareMsg extends JnksIotActorMsg {

    DeviceId getDeviceId();
}
