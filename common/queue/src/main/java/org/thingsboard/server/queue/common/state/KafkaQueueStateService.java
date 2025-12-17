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

@Slf4j
public class KafkaQueueStateService<E extends TbQueueMsg, S extends TbQueueMsg> extends QueueStateService<E, S> {

    // 状态消费者：专门消费“状态快照”主题
    private final PartitionedQueueConsumerManager<S> stateConsumer;

    // 提供事件主题各分区的起始 offset，用于精确 seek（断点续传）
    private final Supplier<Map<String, Long>> eventsStartOffsetsProvider;

    @Builder
    public KafkaQueueStateService(PartitionedQueueConsumerManager<E> eventConsumer,
                                  PartitionedQueueConsumerManager<S> stateConsumer,
                                  Supplier<Map<String, Long>> eventsStartOffsetsProvider) {
        super(eventConsumer);
        this.stateConsumer = stateConsumer;
        this.eventsStartOffsetsProvider = eventsStartOffsetsProvider;
    }

    /**
     * 当系统需要监听新的 Kafka 分区时触发。
     * 流程：先让 stateConsumer 订阅 -> 完成后 -> eventConsumer 订阅。
     */
    @Override
    protected void addPartitions(QueueKey queueKey, Set<TopicPartitionInfo> partitions) {
        // 1) 记录事件主题各分区应该从哪个 offset 开始消费（支持断点续传）
        Map<String, Long> eventsStartOffsets = eventsStartOffsetsProvider != null ? eventsStartOffsetsProvider.get() : null; // remembering the offsets before subscribing to states
        // 2) 把添加的分区映射到stateConsumer的topic下
        Set<TopicPartitionInfo> statePartitions = withTopic(partitions, stateConsumer.getTopic());
        // 3) 标记这些分区为“处理中”，防止并发重复操作
        partitionsInProgress.addAll(statePartitions);
        // 4) 先让 stateConsumer 订阅并消费这些状态分区
        stateConsumer.addPartitions(statePartitions, statePartition -> {
            // 回调：单个状态分区处理完成
            var readLock = partitionsLock.readLock();
            readLock.lock();
            try {
                // 4-1) 去掉“处理中”标记
                partitionsInProgress.remove(statePartition);
                log.info("Finished partition {} (still in progress: {})", statePartition, partitionsInProgress);
                // 4-2) 如果全部状态分区都处理完了，打印提示
                if (partitionsInProgress.isEmpty()) {
                    log.info("All partitions processed");
                }
                // 4-3) 把对应的事件分区也加到 eventConsumer
                TopicPartitionInfo eventPartition = statePartition.withTopic(eventConsumer.getTopic());
                if (this.partitions.get(queueKey).contains(eventPartition)) {
                    eventConsumer.addPartitions(Set.of(eventPartition), null, eventsStartOffsets != null ? eventsStartOffsets::get : null);
                }
            } finally {
                readLock.unlock();
            }
        }, null);
    }

    @Override
    protected void removePartitions(QueueKey queueKey, Set<TopicPartitionInfo> partitions) {
        super.removePartitions(queueKey, partitions);
        stateConsumer.removePartitions(withTopic(partitions, stateConsumer.getTopic()));
    }

    @Override
    protected void deletePartitions(Set<TopicPartitionInfo> partitions) {
        super.deletePartitions(partitions);
        stateConsumer.delete(withTopic(partitions, stateConsumer.getTopic()));
    }

    @Override
    public void stop() {
        super.stop();
        stateConsumer.stop();
        stateConsumer.awaitStop();
    }

}
