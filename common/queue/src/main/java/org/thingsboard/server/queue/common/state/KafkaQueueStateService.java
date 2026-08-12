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

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.queue.TbQueueMsg;
import org.thingsboard.server.queue.common.consumer.PartitionedQueueConsumerManager;
import org.thingsboard.server.queue.discovery.QueueKey;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.thingsboard.server.common.msg.queue.TopicPartitionInfo.withTopic;

/**
 * Kafka 场景下的队列状态服务：在开始消费事件 Topic 之前，先把对应状态 Topic 分区恢复完。
 * <p>
 * 典型用法（计算字段状态、EDQS 等）：业务状态持久化在独立 Kafka Topic 中，分区编号与
 * 事件 Topic 对齐。节点获得某事件分区时，必须先读完同编号的状态分区，再订阅事件，
 * 否则会在本地状态不完整时处理业务消息。
 * <p>
 * <b>新增分区编排（{@link #addPartitions}）：</b>
 * <ol>
 *   <li>可选地快照事件 Topic 各分区起始 offset（订阅状态前记住，避免状态恢复期间事件堆积导致重复/错位点）；</li>
 *   <li>将分区映射到 {@link #stateConsumer} 的 Topic，记入 {@code partitionsInProgress}；</li>
 *   <li>先 {@code stateConsumer.addPartitions}；该分区状态消费结束时触发 {@code onStop}；</li>
 *   <li>回调里若快照仍包含对应事件分区，再以可选起始 offset 订阅 {@code eventConsumer}。</li>
 * </ol>
 * 移除/删除时同时操作事件与状态两侧消费者；删除会物理删两个 Topic 上的对应分区主题。
 *
 * @param <E> 事件（业务）队列消息类型
 * @param <S> 状态队列消息类型
 * @see QueueStateService
 * @see DefaultQueueStateService
 */
@Slf4j
public class KafkaQueueStateService<E extends TbQueueMsg, S extends TbQueueMsg> extends QueueStateService<E, S> {

    /**
     * 状态 Topic 的分区消费者。
     * <p>
     * 与事件 Topic 分区编号对齐；新增分区时优先订阅本消费者，在其分区消费循环结束后
     * （{@code onStop}）再打开对应事件分区。
     */
    private final PartitionedQueueConsumerManager<S> stateConsumer;

    /**
     * 提供「订阅状态之前」事件 Topic 各分区应 seek 到的 offset。
     * <p>
     * key 一般为 Topic 全名；可为 {@code null}，表示不指定起始位点（由消费者默认策略决定）。
     * 在真正 {@code addPartitions} 到状态侧之前取值，避免状态恢复窗口内事件继续推进后位点漂移。
     */
    private final Supplier<Map<String, Long>> eventsStartOffsetsProvider;

    /**
     * @param eventConsumer               事件分区消费者
     * @param stateConsumer               状态分区消费者（Topic 与事件分区编号对齐）
     * @param eventsStartOffsetsProvider  事件起始 offset 供应器，可为 {@code null}
     */
    @Builder
    public KafkaQueueStateService(PartitionedQueueConsumerManager<E> eventConsumer,
                                  PartitionedQueueConsumerManager<S> stateConsumer,
                                  Supplier<Map<String, Long>> eventsStartOffsetsProvider) {
        super(eventConsumer);
        this.stateConsumer = stateConsumer;
        this.eventsStartOffsetsProvider = eventsStartOffsetsProvider;
    }

    /**
     * 先恢复状态分区，完成后再订阅对应事件分区。
     * <p>
     * {@code onStop} 回调在状态分区消费者停止时触发（状态已读完并准备释放该分区消费）。
     * 回调内用读锁检查 {@link #partitions}：若期间 {@link #update}/{@link #delete}
     * 已撤销该事件分区，则不再打开事件消费，避免对已释放分区重复订阅。
     *
     * @param queueKey   触发变更的逻辑队列
     * @param partitions 新增的事件侧分区（方法内会映射到状态 Topic）
     */
    @Override
    protected void addPartitions(QueueKey queueKey, Set<TopicPartitionInfo> partitions) {
        // remembering the offsets before subscribing to states
        Map<String, Long> eventsStartOffsets = eventsStartOffsetsProvider != null ? eventsStartOffsetsProvider.get() : null;
        Set<TopicPartitionInfo> statePartitions = withTopic(partitions, stateConsumer.getTopic());
        partitionsInProgress.addAll(statePartitions);
        stateConsumer.addPartitions(statePartitions, statePartition -> {
            var readLock = partitionsLock.readLock();
            readLock.lock();
            try {
                partitionsInProgress.remove(statePartition);
                log.info("Finished partition {} (still in progress: {})", statePartition, partitionsInProgress);
                if (partitionsInProgress.isEmpty()) {
                    log.info("All partitions processed");
                }
                TopicPartitionInfo eventPartition = statePartition.withTopic(eventConsumer.getTopic());
                if (this.partitions.get(queueKey).contains(eventPartition)) {
                    eventConsumer.addPartitions(Set.of(eventPartition), null, eventsStartOffsets != null ? eventsStartOffsets::get : null);
                }
            } finally {
                readLock.unlock();
            }
        }, null);
    }

    /**
     * 同时从事件消费者与状态消费者上移除分区（不删 Topic）。
     */
    @Override
    protected void removePartitions(QueueKey queueKey, Set<TopicPartitionInfo> partitions) {
        super.removePartitions(queueKey, partitions);
        stateConsumer.removePartitions(withTopic(partitions, stateConsumer.getTopic()));
    }

    /**
     * 同时删除事件 Topic 与状态 Topic 上对应分区主题。
     */
    @Override
    protected void deletePartitions(Set<TopicPartitionInfo> partitions) {
        super.deletePartitions(partitions);
        stateConsumer.delete(withTopic(partitions, stateConsumer.getTopic()));
    }

    /**
     * 先停事件消费者，再停状态消费者，并分别等待退出。
     */
    @Override
    public void stop() {
        super.stop();
        stateConsumer.stop();
        stateConsumer.awaitStop();
    }

}
