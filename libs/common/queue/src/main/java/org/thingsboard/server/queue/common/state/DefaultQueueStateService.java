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
package org.thingsboard.server.queue.common.state;

import org.thingsboard.server.queue.TbQueueMsg;
import org.thingsboard.server.queue.common.consumer.PartitionedQueueConsumerManager;

/**
 * 默认队列状态服务：无独立 Kafka 状态 Topic 时的直通实现。
 * <p>
 * 不覆盖 {@link QueueStateService} 的任何钩子，分区增删/删除 Topic 全部委托给
 * {@code eventConsumer}。适用于状态存在本地（如 RocksDB）或根本不需要「先恢复状态再消费事件」
 * 编排的场景；例如非 Kafka 队列下的计算字段状态服务。
 * <p>
 * 与 {@link KafkaQueueStateService} 的对比：本类没有 {@code stateConsumer}，
 * {@code partitionsInProgress} 始终为空（基类不会写入），{@link #update} 后即可直接消费事件。
 *
 * @param <E> 事件（业务）队列消息类型
 * @param <S> 状态消息类型占位（本实现不使用，仅为与基类泛型签名对齐）
 * @see QueueStateService
 * @see KafkaQueueStateService
 */
public class DefaultQueueStateService<E extends TbQueueMsg, S extends TbQueueMsg> extends QueueStateService<E, S> {

    /**
     * @param eventConsumer 唯一需要管理的事件分区消费者
     */
    public DefaultQueueStateService(PartitionedQueueConsumerManager<E> eventConsumer) {
        super(eventConsumer);
    }

}
