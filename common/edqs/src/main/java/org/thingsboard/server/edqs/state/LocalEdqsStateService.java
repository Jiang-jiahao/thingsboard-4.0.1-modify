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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.ObjectType;
import org.thingsboard.server.common.data.edqs.EdqsEventType;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.edqs.processor.EdqsProcessor;
import org.thingsboard.server.edqs.util.EdqsRocksDb;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdqsMsg;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.common.consumer.PartitionedQueueConsumerManager;
import org.thingsboard.server.queue.edqs.InMemoryEdqsComponent;

import java.util.Set;

import static org.thingsboard.server.common.msg.queue.TopicPartitionInfo.withTopic;

/**
 * 基于本地 RocksDB 的 EDQS 状态服务实现。
 * <p>
 * 在内存 / 单机 EDQS 部署模式下激活（{@link InMemoryEdqsComponent}）。
 * 与 {@link KafkaEdqsStateService} 不同：没有独立的 Kafka state Topic，也没有
 * 「先恢复远程状态再开事件」的多消费者编排；状态直接落在进程本地的 {@link EdqsRocksDb}。
 * <p>
 * <b>生命周期：</b>
 * <ol>
 *   <li>{@link #init}：保存事件消费者引用；</li>
 *   <li>首次 {@link #process}：全量扫描 RocksDB，把历史消息灌回
 *       {@link EdqsProcessor#process(ToEdqsMsg, boolean)}（{@code false}），再
 *       {@code eventConsumer.update} 订阅分区；</li>
 *   <li>后续 {@link #process}：只更新事件消费者分区，不再重复全量恢复；</li>
 *   <li>运行中每次事件处理后由上层调用 {@link #save}，同步更新 / 删除 RocksDB 键值。</li>
 * </ol>
 * <p>
 * 就绪条件简单：只要完成过至少一次 {@link #process}（{@link #partitions} 非 null）即视为就绪。
 * {@link #stop} 为空操作——RocksDB 与事件消费者的关闭由其它组件负责。
 *
 * @see EdqsStateService
 * @see KafkaEdqsStateService
 * @see EdqsRocksDb
 */
@Service
@RequiredArgsConstructor
@InMemoryEdqsComponent
@Slf4j
public class LocalEdqsStateService implements EdqsStateService {

    /**
     * 本地状态存储。
     * <p>
     * key 一般为事件实体键；value 为 {@link ToEdqsMsg} 的序列化字节。
     * 删除事件时移除对应 key。
     */
    private final EdqsRocksDb db;

    /**
     * EDQS 业务处理器。
     * <p>
     * 延迟注入以避免与 {@code EdqsProcessor}（持有本接口）形成循环依赖。
     * 全量恢复与运行时事件最终都落到其 {@code process} 方法。
     */
    @Autowired @Lazy
    private EdqsProcessor processor;

    /**
     * 事件分区消费者。
     * <p>
     * 由 {@link #init} 注入；{@link #process} 中通过 {@code update} 做全量分区对齐
     *（本地模式不需要 Kafka 那套增量 state/event 编排）。
     */
    private PartitionedQueueConsumerManager<TbProtoQueueMsg<ToEdqsMsg>> eventConsumer;

    /**
     * 最近一次 {@link #process} 收到的分区集合快照。
     * <p>
     * {@code null} 表示尚未执行过 process（也作为「尚未做 RocksDB 全量恢复」的标记）；
     * 非 null 后 {@link #isReady} 返回 true，且后续 process 跳过全量恢复。
     */
    private Set<TopicPartitionInfo> partitions;

    /**
     * 保存事件消费者引用，供后续分区更新使用。
     * <p>
     * 本地模式无额外 Kafka state 消费者 / 备份链路需要创建。
     *
     * @param eventConsumer EDQS 事件分区消费者
     */
    @Override
    public void init(PartitionedQueueConsumerManager<TbProtoQueueMsg<ToEdqsMsg>> eventConsumer) {
        this.eventConsumer = eventConsumer;
    }

    /**
     * 对齐分区：首次时先从 RocksDB 恢复全部状态，再更新事件消费者订阅。
     * <p>
     * 仅当 {@link #partitions} 仍为 {@code null} 时执行全量 {@code db.forEach}：
     * 将每条持久化消息反序列化为 {@link ToEdqsMsg} 并交给
     * {@code processor.process(edqsMsg, false)}。单条失败只记日志，不中断整体恢复。
     * <p>
     * 无论是否首次，都会把分区映射到事件 Topic 后调用
     * {@code eventConsumer.update(...)}（全量替换语义），最后缓存本次分区集合。
     *
     * @param partitions 本节点当前应负责的分区
     */
    @Override
    public void process(Set<TopicPartitionInfo> partitions) {
        if (this.partitions == null) {
            // 冷启动：把本地快照全部灌回内存仓库，再开始按分区消费事件
            db.forEach((key, value) -> {
                try {
                    ToEdqsMsg edqsMsg = ToEdqsMsg.parseFrom(value);
                    log.trace("[{}] Restored msg from RocksDB: {}", key, edqsMsg);
                    processor.process(edqsMsg, false);
                } catch (Exception e) {
                    log.error("[{}] Failed to restore value", key, e);
                }
            });
            log.info("Restore completed");
        }
        eventConsumer.update(withTopic(partitions, eventConsumer.getTopic()));
        this.partitions = partitions;
    }

    /**
     * 将事件变更同步写入 RocksDB。
     * <p>
     * {@link EdqsEventType#DELETED} 时删除键；其它类型将整条 {@link ToEdqsMsg}
     * 序列化后 {@code put}。异常仅记录日志，不向调用方抛出，以免打断事件主流程。
     *
     * @param tenantId  租户 ID（日志与排查用；当前存储键以 {@code key} 为准）
     * @param type      对象类型（日志用）
     * @param key       RocksDB 键
     * @param eventType 事件类型，决定 delete 还是 put
     * @param msg       待持久化的完整协议消息
     */
    @Override
    public void save(TenantId tenantId, ObjectType type, String key, EdqsEventType eventType, ToEdqsMsg msg) {
        log.trace("Save to RocksDB: {} {} {} {}", tenantId, type, key, msg);
        try {
            if (eventType == EdqsEventType.DELETED) {
                db.delete(key);
            } else {
                db.put(key, msg.toByteArray());
            }
        } catch (Exception e) {
            log.error("[{}] Failed to save event {}", key, msg, e);
        }
    }

    /**
     * 是否已完成首次分区处理（含 RocksDB 恢复）。
     *
     * @return {@link #partitions} 非 null 时为 true
     */
    @Override
    public boolean isReady() {
        return partitions != null;
    }

    /**
     * 本地实现无可单独停止的备份/状态消费者，故为空操作。
     */
    @Override
    public void stop() {
    }

}
