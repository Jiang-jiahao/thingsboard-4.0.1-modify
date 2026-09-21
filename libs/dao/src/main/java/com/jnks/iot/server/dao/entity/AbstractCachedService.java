package com.jnks.iot.server.dao.entity;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import com.jnks.iot.server.cache.JnksIotTransactionalCache;

import java.io.Serializable;

public abstract class AbstractCachedService<K extends Serializable, V extends Serializable, E> {

    @Autowired
    protected JnksIotTransactionalCache<K, V> cache;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    protected void publishEvictEvent(E event) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            eventPublisher.publishEvent(event);
        } else {
            handleEvictEvent(event);
        }
    }

    public abstract void handleEvictEvent(E event);

}
