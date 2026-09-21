package com.jnks.iot.server.actors.shared;

import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

public class SessionTimeoutCheckMsg implements JnksIotActorMsg {

    private static final SessionTimeoutCheckMsg INSTANCE = new SessionTimeoutCheckMsg();

    private SessionTimeoutCheckMsg() {
    }

    public static SessionTimeoutCheckMsg instance() {
        return INSTANCE;
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.SESSION_TIMEOUT_MSG;
    }
}
