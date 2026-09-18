package org.thingsboard.server.actors.shared;

import org.thingsboard.server.common.msg.MsgType;
import org.thingsboard.server.common.msg.TbActorMsg;

public class SessionTimeoutCheckMsg implements TbActorMsg {

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
