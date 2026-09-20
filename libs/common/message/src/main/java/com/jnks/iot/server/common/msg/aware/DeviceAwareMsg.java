package com.jnks.iot.server.common.msg.aware;

import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.msg.TbActorMsg;

public interface DeviceAwareMsg extends TbActorMsg {

    DeviceId getDeviceId();
}
