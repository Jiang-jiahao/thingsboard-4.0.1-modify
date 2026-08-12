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
 * 队列状态服务基类：在分区发现结果与实际消费者订阅之间做协调。
 * <p>
 * 上层（规则引擎计算字段、EDQS 等）在收到分区变更时调用 {@link #update}，本类负责：
 * <ul>
 *   <li>按 {@link QueueKey} 维护本节点当前应持有的分区快照；</li>
 *   <li>相对旧快照做差集，驱动事件消费者增量 {@code add}/{@code remove}；</li>
 *   <li>在实体/租户销毁等场景通过 {@link #delete} 停止消费并删除底层 Topic。</li>
 * </ul>
 * <p>
 * 本身不实现「先恢复状态再消费事件」等编排；Kafka 场景由
 * {@link KafkaQueueStateService} 覆盖钩子完成，内存/RocksDB 等无独立状态 Topic 的场景
 * 使用 {@link DefaultQueueStateService} 直接委托 {@link #eventConsumer}。
 * <p>
 * <b>并发模型：</b>{@link #partitions} 的读写由 {@link #partitionsLock} 保护；
 * 真正对消费者的增删在锁外调用，避免长时间占用写锁。{@link #partitionsInProgress}
 * 供子类标记「状态恢复尚未完成」的分区，供上游判断是否可开始处理业务流量。
 *
 * @param <E> 事件（业务）队列消息类型
 * @param <S> 状态队列消息类型（无状态 Topic 时可为与 {@code E} 相同的占位类型）
 * @see PartitionedQueueConsumerManager
 * @see KafkaQueueStateService
 * @see DefaultQueueStateService
 */
@Slf4j
public abstract class QueueStateService<E extends TbQueueMsg, S extends TbQueueMsg> {

    /**
     * 事件（业务）分区消费者管理器。
     * <p>
     * 负责按分区拉取业务消息；基类默认的增删/删除 Topic 都落到该实例上。
     */
    protected final PartitionedQueueConsumerManager<E> eventConsumer;

    /**
     * 各逻辑队列当前持有的分区快照。
     * <p>
     * key 为 {@link QueueKey}（服务类型 + 队列名等）；value 为本节点对该队列应订阅的
     * {@link TopicPartitionInfo} 全集。{@link #update} 会整体替换某一 key 的集合；
     * {@link #delete} 会从所有 value 中移除指定分区。
     */
    @Getter
    protected final Map<QueueKey, Set<TopicPartitionInfo>> partitions = new HashMap<>();

    /**
     * 仍在「处理中」的分区集合（通常表示状态恢复尚未完成）。
     * <p>
     * 基类不写入；{@link KafkaQueueStateService} 在订阅状态分区时加入，状态消费结束后移除。
     * {@link #getPartitionsInProgress()} 在已初始化时返回本集合，未初始化时返回 {@code null}。
     */
    protected final Set<TopicPartitionInfo> partitionsInProgress = ConcurrentHashMap.newKeySet();

    /**
     * 是否至少成功执行过一次 {@link #update}。
     * <p>
     * 用于区分「尚未拿到任何分区分配」与「已初始化但当前无处理中分区」。
     */
    protected boolean initialized;

    /**
     * 保护 {@link #partitions} 的读写锁。
     * <p>
     * {@link #update}/{@link #delete} 用写锁更新快照；子类在回调中查询快照时用读锁，
     * 避免与并发分区变更交叉导致误判。
     */
    protected final ReadWriteLock partitionsLock = new ReentrantReadWriteLock();

    /**
     * @param eventConsumer 事件分区消费者；后续默认增删/删 Topic 均委托给它
     */
    protected QueueStateService(PartitionedQueueConsumerManager<E> eventConsumer) {
        this.eventConsumer = eventConsumer;
    }

    /**
     * 按发现服务给出的最新分区全集，对齐某一 {@link QueueKey} 的订阅。
     * <p>
     * 流程：
     * <ol>
     *   <li>将分区统一改写到 {@link #eventConsumer} 的 Topic（保证 full topic name 一致）；</li>
     *   <li>在写锁下相对旧快照计算 added / removed，并覆盖该 key 的快照；</li>
     *   <li>锁外先 {@link #removePartitions} 再 {@link #addPartitions}（子类可覆盖编排）；</li>
     *   <li>标记 {@link #initialized} = true。</li>
     * </ol>
     * 注意：一次调用只更新单个 {@code queueKey}；分区会先映射到事件 Topic，
     * 不宜混入其它 Topic 的分区集合。
     *
     * @param queueKey      逻辑队列标识
     * @param newPartitions 该队列最新应持有的分区全集（Topic 可随后被改写）
     */
    public void update(QueueKey queueKey, Set<TopicPartitionInfo> newPartitions) {
        newPartitions = withTopic(newPartitions, eventConsumer.getTopic());
        var writeLock = partitionsLock.writeLock();
        writeLock.lock();
        Set<TopicPartitionInfo> oldPartitions = this.partitions.getOrDefault(queueKey, Collections.emptySet());
        Set<TopicPartitionInfo> addedPartitions;
        Set<TopicPartitionInfo> removedPartitions;
        try {
            addedPartitions = new HashSet<>(newPartitions);
            addedPartitions.removeAll(oldPartitions);
            removedPartitions = new HashSet<>(oldPartitions);
            removedPartitions.removeAll(newPartitions);
            this.partitions.put(queueKey, newPartitions);
        } finally {
            writeLock.unlock();
        }
        if (!removedPartitions.isEmpty()) {
            removePartitions(queueKey, removedPartitions);
        }
        if (!addedPartitions.isEmpty()) {
            addPartitions(queueKey, addedPartitions);
        }
        initialized = true;
    }

    /**
     * 将新增分区注册到事件消费者（子类可覆盖以实现状态恢复等编排）。
     * <p>
     * 默认实现：直接 {@code eventConsumer.addPartitions(partitions)}。
     *
     * @param queueKey   触发本次变更的逻辑队列
     * @param partitions 相对旧快照新增的分区（已映射到事件 Topic）
     */
    protected void addPartitions(QueueKey queueKey, Set<TopicPartitionInfo> partitions) {
        eventConsumer.addPartitions(partitions);
    }

    /**
     * 将移除分区从事件消费者注销（子类可覆盖以同时处理状态消费者）。
     * <p>
     * 默认实现：仅停止事件侧消费，不删除底层 Topic。
     *
     * @param queueKey   触发本次变更的逻辑队列
     * @param partitions 相对旧快照移除的分区（已映射到事件 Topic）
     */
    protected void removePartitions(QueueKey queueKey, Set<TopicPartitionInfo> partitions) {
        eventConsumer.removePartitions(partitions);
    }

    /**
     * 从所有逻辑队列的快照中移除指定分区，并删除对应底层 Topic。
     * <p>
     * 与 {@link #update} 算出的 remove 不同：这里面向「分区生命周期结束」
     * （如租户/实体销毁），会调用 {@link #deletePartitions} 做物理清理。
     * 传入的分区会按事件 Topic 改写后再删。
     *
     * @param partitions 待删除的分区（通常只含 partition id 等信息，Topic 由本方法补齐）
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
     * 停止事件消费者并删除其 Topic（子类可覆盖以同时删除状态 Topic）。
     *
     * @param partitions 待删除分区（方法内会 {@code withTopic} 到事件 Topic）
     */
    protected void deletePartitions(Set<TopicPartitionInfo> partitions) {
        eventConsumer.delete(withTopic(partitions, eventConsumer.getTopic()));
    }

    /**
     * 返回仍在处理中的分区；若从未 {@link #update} 过则返回 {@code null}。
     * <p>
     * 上游可用 {@code null} 表示「尚未就绪」，用空集合表示「已就绪且无恢复中分区」。
     *
     * @return 处理中分区集合，或 {@code null}（未初始化）
     */
    public Set<TopicPartitionInfo> getPartitionsInProgress() {
        return initialized ? partitionsInProgress : null;
    }

    /**
     * 停止事件消费者并阻塞等待其退出。
     * <p>
     * 持有额外消费者（如状态 Topic）的子类应覆盖并一并停止。
     */
    public void stop() {
        eventConsumer.stop();
        eventConsumer.awaitStop();
    }

}
