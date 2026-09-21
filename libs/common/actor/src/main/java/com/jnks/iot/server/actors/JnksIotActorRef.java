package com.jnks.iot.server.actors;

import com.jnks.iot.server.common.msg.JnksIotActorMsg;

public interface JnksIotActorRef {

    JnksIotActorId getActorId();

    void tell(JnksIotActorMsg actorMsg);

    void tellWithHighPriority(JnksIotActorMsg actorMsg);

}
