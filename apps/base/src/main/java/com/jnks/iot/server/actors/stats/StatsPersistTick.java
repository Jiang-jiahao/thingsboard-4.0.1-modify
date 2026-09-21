package com.jnks.iot.server.actors.stats;

import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

public enum StatsPersistTick implements JnksIotActorMsg {
    INSTANCE;

    @Override
    public MsgType getMsgType() {
        return MsgType.STATS_PERSIST_TICK_MSG;
    }
}
