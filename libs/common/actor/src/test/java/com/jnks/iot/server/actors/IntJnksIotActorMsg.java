package com.jnks.iot.server.actors;

import lombok.Getter;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

public class IntJnksIotActorMsg implements JnksIotActorMsg {

    @Getter
    private final int value;

    public IntJnksIotActorMsg(int value) {
        this.value = value;
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.QUEUE_TO_RULE_ENGINE_MSG;
    }
}
