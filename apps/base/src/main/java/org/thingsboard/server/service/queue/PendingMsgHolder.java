package org.thingsboard.server.service.queue;

import lombok.Getter;
import lombok.Setter;

public class PendingMsgHolder<T> {
    @Getter @Setter
    private volatile T msg;
}
