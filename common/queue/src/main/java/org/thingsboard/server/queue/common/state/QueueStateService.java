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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.queue.TbQueueMsg;
import org.thingsboard.server.queue.common.consumer.PartitionedQueueConsumerManager;
import org.thingsboard.server.queue.discovery.QueueKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.thingsboard.server.common.msg.queue.TopicPartitionInfo.withTopic;

/**
 * 队列状态服务 - 负责分布式系统中队列状态的统一管理和协调
 *
 * 主要维护各个topic下的分区信息，并管理分区的添加、删除、迁移等操作。
 */
@Slf4j
public abstract class QueueStateService<E extends TbQueueMsg, S extends TbQueueMsg> {

    // 分区级消费者管理器（实际负责从队列里拉消息）
    protected final PartitionedQueueConsumerManager<E> eventConsumer;

    // key：每个 QueueKey 对应一个逻辑消费单元（可能是一个服务实例或消费者组）   value：该队列当前持有的所有分区
    @Getter
    protected final Map<QueueKey, Set<TopicPartitionInfo>> partitions = new HashMap<>();

    // 正在“处理中”的分区集合（例如正在初始化/关闭/迁移的分区）
    protected final Set<TopicPartitionInfo> partitionsInProgress = ConcurrentHashMap.newKeySet();

    // 是否已经过一次 update 初始化
    protected boolean initialized;

    // 读写锁，保证 partitions 的并发安全
    protected final ReadWriteLock partitionsLock = new ReentrantReadWriteLock();

    protected QueueStateService(PartitionedQueueConsumerManager<E> eventConsumer) {
        this.eventConsumer = eventConsumer;
    }

    /**
     * 根据新的分区集合，更新某个队列的分区信息。(只能处理单一topic的分区数据)
     * 会计算出“新增”与“删除”的分区，并分别调用 add/remove 方法。
     *
     * @param queueKey      队列唯一标识
     * @param newPartitions 新的分区集合
     */
    public void update(QueueKey queueKey, Set<TopicPartitionInfo> newPartitions) {
        // 1. 为所有分区补上 topic 属性，保证全名一致
        newPartitions = withTopic(newPartitions, eventConsumer.getTopic());
        // 2. 获取写锁，防止并发修改 partitions
        var writeLock = partitionsLock.writeLock();
        writeLock.lock();
        // 3. 计算“新增”与“删除”的差集
        Set<TopicPartitionInfo> oldPartitions = this.partitions.getOrDefault(queueKey, Collections.emptySet());
        Set<TopicPartitionInfo> addedPartitions;
        Set<TopicPartitionInfo> removedPartitions;
        try {
            addedPartitions = new HashSet<>(newPartitions);
            // 新增分区 = 新集合 - 旧集合
            addedPartitions.removeAll(oldPartitions);
            removedPartitions = new HashSet<>(oldPartitions);
            // 删除分区 = 旧集合 - 新集合
            removedPartitions.removeAll(newPartitions);
            // 4. 用新集合覆盖旧集合
            this.partitions.put(queueKey, newPartitions);
        } finally {
            writeLock.unlock();
        }
        // 5. 如果存在要删除的分区，则调用具体实现进行删除
        if (!removedPartitions.isEmpty()) {
            removePartitions(queueKey, removedPartitions);
        }
        // 6. 如果存在要新增的分区，则调用具体实现进行添加
        if (!addedPartitions.isEmpty()) {
            addPartitions(queueKey, addedPartitions);
        }
        initialized = true;
    }

    /**
     * 子类实现：真正把新增的分区注册到消费者
     *
     * @param queueKey      队列唯一标识
     * @param partitions 要新增的分区集合
     */
    protected void addPartitions(QueueKey queueKey, Set<TopicPartitionInfo> partitions) {
        eventConsumer.addPartitions(partitions);
    }

    /**
     * 子类实现：真正把删除的分区从消费者注销
     *
     * @param queueKey      队列唯一标识
     * @param partitions 要删除的分区集合
     */
    protected void removePartitions(QueueKey queueKey, Set<TopicPartitionInfo> partitions) {
        eventConsumer.removePartitions(partitions);
    }

    /**
     * 删除所有队列下的指定分区
     *
     * @param partitions 要删除的分区集合
     */
    public void delete(Set<TopicPartitionInfo> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        var writeLock = partitionsLock.writeLock();
        writeLock.lock();
        try {
            this.partitions.values().forEach(tpis -> tpis.removeAll(partitions));
        } finally {
            writeLock.unlock();
        }
        deletePartitions(partitions);
    }

    /**
     * 子类实现：真正把分区从底层消息队列中删除
     */
    protected void deletePartitions(Set<TopicPartitionInfo> partitions) {
        eventConsumer.delete(withTopic(partitions, eventConsumer.getTopic()));
    }

    /**
     * 获取当前“处理中”的分区集合。
     * 如果还没初始化，则返回 null。
     */
    public Set<TopicPartitionInfo> getPartitionsInProgress() {
        return initialized ? partitionsInProgress : null;
    }

    /**
     * 优雅关闭：停止消费者并等待其完全退出
     */
    public void stop() {
        eventConsumer.stop();
        eventConsumer.awaitStop();
    }

}
