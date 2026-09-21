package com.jnks.iot.server.actors;

import lombok.Getter;

public class JnksIotActorNotRegisteredException extends RuntimeException {

    @Getter
    private JnksIotActorId target;

    public JnksIotActorNotRegisteredException(JnksIotActorId target, String message) {
        super(message);
        this.target = target;
    }
}
