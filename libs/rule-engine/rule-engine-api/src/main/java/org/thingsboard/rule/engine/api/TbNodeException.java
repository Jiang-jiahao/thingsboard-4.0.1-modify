package org.thingsboard.rule.engine.api;

import lombok.Getter;
import org.thingsboard.server.common.msg.TbActorError;

/**
 * Created by ashvayka on 19.01.18.
 */
public class TbNodeException extends Exception implements TbActorError {

    @Getter
    private final boolean unrecoverable;

    public TbNodeException(String message) {
        this(message, false);
    }

    public TbNodeException(String message, boolean unrecoverable) {
        super(message);
        this.unrecoverable = unrecoverable;
    }

    public TbNodeException(Exception e) {
        this(e, false);
    }

    public TbNodeException(Exception e, boolean unrecoverable) {
        super(e);
        this.unrecoverable = unrecoverable;
    }

}
