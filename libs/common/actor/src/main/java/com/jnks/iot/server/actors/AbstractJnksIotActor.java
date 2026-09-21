package com.jnks.iot.server.actors;

import lombok.Getter;

public abstract class AbstractJnksIotActor implements JnksIotActor {

    @Getter
    protected JnksIotActorCtx ctx;

    @Override
    public void init(JnksIotActorCtx ctx) throws JnksIotActorException {
        this.ctx = ctx;
    }

    @Override
    public JnksIotActorRef getActorRef() {
        return ctx;
    }
}
