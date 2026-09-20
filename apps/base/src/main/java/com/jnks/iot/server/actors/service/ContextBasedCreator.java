package com.jnks.iot.server.actors.service;

import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.TbActorCreator;

public abstract class ContextBasedCreator implements TbActorCreator {

    protected final transient ActorSystemContext context;

    public ContextBasedCreator(ActorSystemContext context) {
        super();
        this.context = context;
    }
}
