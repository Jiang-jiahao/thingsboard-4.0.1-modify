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
package org.thingsboard.server.edqs.state;

import org.thingsboard.server.common.data.ObjectType;
import org.thingsboard.server.common.data.edqs.EdqsEventType;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdqsMsg;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.common.consumer.PartitionedQueueConsumerManager;

import java.util.Set;

/**
 * EDQS（Entity Data Query Service）状态服务抽象。
 * <p>
 * 负责在节点获得分区所有权后，把「持久化的 EDQS 状态」恢复到内存仓库，并决定何时可以
 * 开始消费实时事件 / 对外提供查询。具体持久化介质与恢复编排由实现类决定：
 * <ul>
 *   <li>{@link KafkaEdqsStateService}：状态落在 Kafka state Topic，经
 *       {@code KafkaQueueStateService}「先读状态、再开事件」编排；</li>
 *   <li>{@link LocalEdqsStateService}：状态落在本地 RocksDB，首次分区分配时全量扫描恢复，
 *       之后每次变更由 {@link #save} 同步写入。</li>
 * </ul>
 * <p>
 * 典型调用方为 {@code EdqsProcessor}：
 * <ol>
 *   <li>构建事件消费者后调用 {@link #init}；</li>
 *   <li>收到分区变更事件时调用 {@link #process}；</li>
 *   <li>处理事件时按需调用 {@link #save}（Kafka 实现通常为空操作）；</li>
 *   <li>用 {@link #isReady} 判断是否已完成至少一轮状态恢复、可对外提供服务。</li>
 * </ol>
 *
 * @see KafkaEdqsStateService
 * @see LocalEdqsStateService
 */
public interface EdqsStateService {

    /**
     * 注入 EDQS 事件分区消费者，并完成实现类内部组件的初始化。
     * <p>
     * 调用时机：事件消费者已构建完成，但通常尚未按分区启动订阅。
     * Kafka 实现会在此创建 state 消费者、events-to-backup 消费者、state 生产者以及
     * {@code KafkaQueueStateService}；本地实现一般只保存引用。
     *
     * @param eventConsumer 按分区消费 EDQS 事件 Topic 的管理器，后续分区对齐会驱动其增删订阅
     */
    void init(PartitionedQueueConsumerManager<TbProtoQueueMsg<ToEdqsMsg>> eventConsumer);

    /**
     * 根据最新分区分配结果驱动状态恢复与事件消费对齐。
     * <p>
     * 由分区变更事件触发。实现类应保证：在开始（或继续）消费对应事件分区之前，
     * 本节点负责分区上的历史状态已尽可能恢复完毕。传入的分区集合通常已带上状态/事件
     * Topic 信息，具体映射由实现处理。
     *
     * @param partitions 本节点当前应负责的分区集合（来自分区发现）
     */
    void process(Set<TopicPartitionInfo> partitions);

    /**
     * 将一条已处理（或待持久化）的 EDQS 事件写入状态存储。
     * <p>
     * 本地实现据此维护 RocksDB 快照；Kafka 实现通常不做任何事——状态备份由独立的
     * events-to-backup 消费链路异步写入 state Topic，避免与主处理路径重复写。
     *
     * @param tenantId  租户 ID
     * @param type      对象类型（设备、资产等）
     * @param key       状态键（通常与事件中的实体键一致）
     * @param eventType 事件类型（如 UPDATED / DELETED）；删除时实现应清理对应键
     * @param msg       完整的 EDQS 协议消息，便于原样持久化或转发
     */
    void save(TenantId tenantId, ObjectType type, String key, EdqsEventType eventType, ToEdqsMsg msg);

    /**
     * 判断 EDQS 状态是否已就绪，可对外提供查询等服务。
     * <p>
     * 「就绪」一般表示至少完成过一轮状态恢复。Kafka 实现在首次
     * {@code partitionsInProgress} 变空后置位且通常不再回退；本地实现在首次
     * {@link #process} 完成 RocksDB 恢复并记录分区后即为 true。
     *
     * @return {@code true} 表示已就绪；{@code false} 表示仍在初始化或尚未拿到分区
     */
    boolean isReady();

    /**
     * 停止本服务持有的消费者、生产者等资源。
     * <p>
     * 进程关闭或组件销毁时调用。不持有额外资源的实现可以为空操作。
     */
    void stop();

}
