package com.jnks.iot.server.actors;

import lombok.Getter;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.TbActorMsg;

public class IntTbActorMsg implements TbActorMsg {

    @Getter
    private final int value;

    public IntTbActorMsg(int value) {
        this.value = value;
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.QUEUE_TO_RULE_ENGINE_MSG;
    }
}
