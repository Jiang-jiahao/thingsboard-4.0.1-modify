package com.jnks.iot.server.actors.app;

import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.TbActorMsg;

public class AppInitMsg implements TbActorMsg {

    @Override
    public MsgType getMsgType() {
        return MsgType.APP_INIT_MSG;
    }
}
