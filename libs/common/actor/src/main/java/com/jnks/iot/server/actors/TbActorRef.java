package com.jnks.iot.server.actors;

import com.jnks.iot.server.common.msg.TbActorMsg;

public interface TbActorRef {

    TbActorId getActorId();

    void tell(TbActorMsg actorMsg);

    void tellWithHighPriority(TbActorMsg actorMsg);

}
