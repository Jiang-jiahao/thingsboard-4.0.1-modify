package org.thingsboard.server.dao.entity;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.thingsboard.server.cache.TbTransactionalCache;

import java.io.Serializable;

public abstract class AbstractCachedEntityService<K extends Serializable, V extends Serializable, E> extends AbstractEntityService {

    @Autowired
    protected TbTransactionalCache<K, V> cache;

    protected void publishEvictEvent(E event) {
        // 判断当前线程是否在事务中
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            // 如果运行在事务上下文中，那么直接发布事件。（延迟清理缓存）
            eventPublisher.publishEvent(event);
        } else {
            // 如果不在事务中，那么直接调用handleEvictEvent方法处理事件。（立即清理缓存）
            handleEvictEvent(event);
        }
    }

    public abstract void handleEvictEvent(E event);

}
