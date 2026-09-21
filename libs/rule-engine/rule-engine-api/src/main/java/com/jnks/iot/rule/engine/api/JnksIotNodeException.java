package com.jnks.iot.rule.engine.api;

import lombok.Getter;
import com.jnks.iot.server.common.msg.JnksIotActorError;

/**
 * Created by ashvayka on 19.01.18.
 */
public class JnksIotNodeException extends Exception implements JnksIotActorError {

    @Getter
    private final boolean unrecoverable;

    public JnksIotNodeException(String message) {
        this(message, false);
    }

    public JnksIotNodeException(String message, boolean unrecoverable) {
        super(message);
        this.unrecoverable = unrecoverable;
    }

    public JnksIotNodeException(Exception e) {
        this(e, false);
    }

    public JnksIotNodeException(Exception e, boolean unrecoverable) {
        super(e);
        this.unrecoverable = unrecoverable;
    }

}
