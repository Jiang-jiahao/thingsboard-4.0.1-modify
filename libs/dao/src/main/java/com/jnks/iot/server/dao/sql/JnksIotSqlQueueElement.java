package com.jnks.iot.server.dao.sql;

import com.google.common.util.concurrent.SettableFuture;
import lombok.Getter;
import lombok.ToString;

@ToString(exclude = "future")
public final class JnksIotSqlQueueElement<E, R> {
    @Getter
    private final SettableFuture<R> future;
    @Getter
    private final E entity;

    public JnksIotSqlQueueElement(SettableFuture<R> future, E entity) {
        this.future = future;
        this.entity = entity;
    }
}


