package com.jnks.iot.server.actors.app;

import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

public class AppInitMsg implements JnksIotActorMsg {

    @Override
    public MsgType getMsgType() {
        return MsgType.APP_INIT_MSG;
    }
}
