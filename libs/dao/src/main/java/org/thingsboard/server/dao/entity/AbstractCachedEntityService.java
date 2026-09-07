/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
