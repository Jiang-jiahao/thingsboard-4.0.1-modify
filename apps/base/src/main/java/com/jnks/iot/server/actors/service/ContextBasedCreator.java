package com.jnks.iot.server.actors.service;

import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActorCreator;

public abstract class ContextBasedCreator implements JnksIotActorCreator {

    protected final transient ActorSystemContext context;

    public ContextBasedCreator(ActorSystemContext context) {
        super();
        this.context = context;
    }
}
