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
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.edqs.processor.EdqsProcessor;
import org.thingsboard.server.edqs.processor.EdqsProducer;
import org.thingsboard.server.edqs.util.VersionsStore;
import org.thingsboard.server.gen.transport.TransportProtos.EdqsEventMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdqsMsg;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.common.consumer.PartitionedQueueConsumerManager;
import org.thingsboard.server.queue.common.consumer.QueueConsumerManager;
import org.thingsboard.server.queue.common.state.KafkaQueueStateService;
import org.thingsboard.server.queue.common.state.QueueStateService;
import org.thingsboard.server.queue.discovery.QueueKey;
import org.thingsboard.server.queue.edqs.EdqsConfig;
import org.thingsboard.server.queue.edqs.KafkaEdqsComponent;
import org.thingsboard.server.queue.edqs.KafkaEdqsQueueFactory;
import org.thingsboard.server.queue.kafka.TbKafkaAdmin;
import org.thingsboard.server.queue.kafka.TbKafkaConsumerTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 基于 Kafka 的 EDQS 状态服务实现。
 * <p>
 * 在 Kafka EDQS 部署模式下激活（{@link KafkaEdqsComponent}）。状态不以本地磁盘为主，
 * 而是持久化在独立的 <b>state Topic</b> 中；节点拿到分区后必须先把对应 state 分区读入
 * 内存仓库，再订阅 <b>events Topic</b>，否则会在状态不完整时处理实时事件。
 * <p>
 * <b>核心组件与数据流：</b>
 * <ul>
 *   <li>{@link #stateConsumer}：按分区消费 state Topic，消息交给
 *       {@link EdqsProcessor#process(ToEdqsMsg, boolean)}（{@code false} 表示非实时事件路径）；</li>
 *   <li>{@link #eventsToBackupConsumer}：消费 events Topic 全部分区，将事件写回 state Topic
 *       （备份），并用 {@link #versionsStore} 按版本去重；</li>
 *   <li>{@link #stateProducer}：向 state Topic 发送备份消息；</li>
 *   <li>{@link #queueStateService}：{@link KafkaQueueStateService}，编排
 *       「先 state 恢复 → 再打开 event 分区」，并在打开事件消费前注入起始 offset。</li>
 * </ul>
 * <p>
 * <b>为何需要 events-to-backup：</b>
 * 事件消费者本身不使用 Kafka consumer group 管理（以便多实例消费同一 topic-partition），
 * 因此无法可靠跟踪 offset。备份消费组承担两件事：把事件固化到 state Topic，以及提供
 * 「恢复完成后事件应从何处开始消费」的 offset 快照。
 * <p>
 * {@link #save} 故意为空：主处理路径不写状态；状态由备份消费者异步维护，避免双写。
 *
 * @see EdqsStateService
 * @see LocalEdqsStateService
 * @see KafkaQueueStateService
 */
@Service
@RequiredArgsConstructor
@KafkaEdqsComponent
@Slf4j
public class KafkaEdqsStateService implements EdqsStateService {

    /**
     * EDQS 队列与 Topic 相关配置（events/state Topic 名、分区数、poll 间隔等）。
     */
    private final EdqsConfig config;

    /**
     * 分区解析服务：按租户/实体等计算消息应发往的分区。
     * <p>
     * 供 {@link #stateProducer} 在写入 state Topic 时选择分区。
     */
    private final EdqsPartitionService partitionService;

    /**
     * Kafka 版 EDQS 队列工厂：创建 state/events 消费者、生产者以及 {@link TbKafkaAdmin}。
     */
    private final KafkaEdqsQueueFactory queueFactory;

    /**
     * EDQS 业务处理器。
     * <p>
     * 延迟注入以避免与 {@code EdqsProcessor} 互相依赖形成的循环依赖
     * （Processor 持有 {@link EdqsStateService}，本类又要回调 Processor）。
     */
    @Autowired
    @Lazy
    private EdqsProcessor edqsProcessor;

    /**
     * 按分区消费 state Topic 的管理器。
     * <p>
     * 由 {@link KafkaQueueStateService} 在分区分配时优先启动；每个分区读完并停止后，
     * 再打开对应的事件分区消费。
     */
    private PartitionedQueueConsumerManager<TbProtoQueueMsg<ToEdqsMsg>> stateConsumer;

    /**
     * 事件与状态消费者的协调器。
     * <p>
     * {@link #process} 最终调用其 {@code update}；内部负责相对旧快照做增删，
     * 并在 Kafka 场景下保证状态恢复完成前不订阅事件分区。
     */
    private QueueStateService<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<ToEdqsMsg>> queueStateService;

    /**
     * 将 events Topic 消息备份到 state Topic 的消费者（单消费者订阅全部分区）。
     * <p>
     * 首次 {@link #process} 时订阅并启动；同时其 consumer group 的 committed offset
     * 作为事件侧起始位点的来源。
     */
    private QueueConsumerManager<TbProtoQueueMsg<ToEdqsMsg>> eventsToBackupConsumer;

    /**
     * 向 state Topic 发送备份消息的生产者封装。
     */
    private EdqsProducer stateProducer;

    /**
     * 备份路径上的事件版本去重存储。
     * <p>
     * 若事件带 version 且不是更新的版本，则跳过写入 state，避免旧事件覆盖新状态。
     */
    private final VersionsStore versionsStore = new VersionsStore();

    /**
     * 已从 state Topic 处理的消息计数（用于周期性 info 日志）。
     */
    private final AtomicInteger stateReadCount = new AtomicInteger();

    /**
     * 已从 events-to-backup 路径处理的消息计数（用于周期性 info 日志）。
     */
    private final AtomicInteger eventsReadCount = new AtomicInteger();

    /**
     * 就绪标志缓存。
     * <p>
     * {@code null} 表示尚未判定；首次变为 {@code true} 后保持为 true，
     * 避免后续重平衡时因短暂的 {@code partitionsInProgress} 非空而抖动就绪状态。
     */
    private Boolean ready;

    /**
     * 初始化 Kafka 状态恢复与备份所需的全部组件。
     * <p>
     * 步骤：
     * <ol>
     *   <li>构建 {@link #stateConsumer}：消费 state Topic，逐条交给
     *       {@code edqsProcessor.process(msg, false)}，并 commit；</li>
     *   <li>构建 {@link #eventsToBackupConsumer}：消费 events，按版本过滤后
     *       {@link EdqsProducer#send} 写入 state；</li>
     *   <li>构建 {@link #stateProducer}；</li>
     *   <li>组装 {@link KafkaQueueStateService}，注入事件起始 offset 供应器——
     *       从备份消费组已提交 offset 读取（因事件主消费者不走 group 管理）。</li>
     * </ol>
     * 本方法不启动备份消费者、也不订阅分区；真正的分区对齐在 {@link #process} 中触发。
     *
     * @param eventConsumer 由 {@code EdqsProcessor} 创建的事件分区消费者，线程池与错误处理与其共享
     */
    @Override
    public void init(PartitionedQueueConsumerManager<TbProtoQueueMsg<ToEdqsMsg>> eventConsumer) {
        TbKafkaAdmin queueAdmin = queueFactory.getEdqsQueueAdmin();
        stateConsumer = PartitionedQueueConsumerManager.<TbProtoQueueMsg<ToEdqsMsg>>create()
                .queueKey(new QueueKey(ServiceType.EDQS, config.getStateTopic()))
                .topic(config.getStateTopic())
                .pollInterval(config.getPollInterval())
                .msgPackProcessor((msgs, consumer, config) -> {
                    for (TbProtoQueueMsg<ToEdqsMsg> queueMsg : msgs) {
                        try {
                            ToEdqsMsg msg = queueMsg.getValue();
                            // false：状态恢复路径，与实时事件路径在 Processor 内可区分处理策略
                            edqsProcessor.process(msg, false);
                            if (stateReadCount.incrementAndGet() % 100000 == 0) {
                                log.info("[state] Processed {} msgs", stateReadCount.get());
                            }
                        } catch (Exception e) {
                            log.error("Failed to process message: {}", queueMsg, e);
                        }
                    }
                    consumer.commit();
                })
                .consumerCreator((config, tpi) -> queueFactory.createEdqsStateConsumer())
                .queueAdmin(queueAdmin)
                .consumerExecutor(eventConsumer.getConsumerExecutor())
                .taskExecutor(eventConsumer.getTaskExecutor())
                .scheduler(eventConsumer.getScheduler())
                .uncaughtErrorHandler(edqsProcessor.getErrorHandler())
                .build();

        TbKafkaConsumerTemplate<TbProtoQueueMsg<ToEdqsMsg>> eventsToBackupKafkaConsumer = queueFactory.createEdqsEventsToBackupConsumer();
        eventsToBackupConsumer = QueueConsumerManager.<TbProtoQueueMsg<ToEdqsMsg>>builder()
                .name("edqs-events-to-backup-consumer")
                .pollInterval(config.getPollInterval())
                .msgPackProcessor((msgs, consumer) -> {
                    for (TbProtoQueueMsg<ToEdqsMsg> queueMsg : msgs) {
                        if (consumer.isStopped()) {
                            return;
                        }
                        try {
                            ToEdqsMsg msg = queueMsg.getValue();
                            log.trace("Processing message: {}", msg);

                            if (msg.hasEventMsg()) {
                                EdqsEventMsg eventMsg = msg.getEventMsg();
                                String key = eventMsg.getKey();
                                int count = eventsReadCount.incrementAndGet();
                                if (count % 100000 == 0) {
                                    log.info("[events-to-backup] Processed {} msgs", count);
                                }
                                // 带版本时做去重：旧版本不写回 state，防止状态回退
                                if (eventMsg.hasVersion()) {
                                    if (!versionsStore.isNew(key, eventMsg.getVersion())) {
                                        continue;
                                    }
                                }

                                TenantId tenantId = getTenantId(msg);
                                ObjectType objectType = ObjectType.valueOf(eventMsg.getObjectType());
                                EdqsEventType eventType = EdqsEventType.valueOf(eventMsg.getEventType());
                                log.trace("[{}] Saving to backup [{}] [{}] [{}]", tenantId, objectType, eventType, key);
                                stateProducer.send(tenantId, objectType, key, msg);
                            }
                        } catch (Throwable t) {
                            log.error("Failed to process message: {}", queueMsg, t);
                        }
                    }
                    consumer.commit();
                })
                .consumerCreator(() -> eventsToBackupKafkaConsumer)
                .consumerExecutor(eventConsumer.getConsumerExecutor())
                .threadPrefix("edqs-events-to-backup")
                .build();

        stateProducer = EdqsProducer.builder()
                .producer(queueFactory.createEdqsStateProducer())
                .partitionService(partitionService)
                .build();

        queueStateService = KafkaQueueStateService.<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<ToEdqsMsg>>builder()
                .eventConsumer(eventConsumer)
                .stateConsumer(stateConsumer)
                .eventsStartOffsetsProvider(() -> {
                    // 事件主消费者不用 consumer group，无法自己跟踪 offset；
                    // 因此从 events-to-backup 消费组的已提交位点，作为恢复完成后事件分区的起始 offset
                    Map<String, Long> offsets = new HashMap<>();
                    try {
                        queueAdmin.getConsumerGroupOffsets(eventsToBackupKafkaConsumer.getGroupId())
                                .forEach((topicPartition, offsetAndMetadata) -> {
                                    offsets.put(topicPartition.topic(), offsetAndMetadata.offset());
                                });
                    } catch (Exception e) {
                        log.error("Failed to get consumer group offsets for {}", eventsToBackupKafkaConsumer.getGroupId(), e);
                    }
                    return offsets;
                })
                .build();
    }

    /**
     * 响应分区变更：必要时启动全量备份消费，并委托 {@link #queueStateService} 对齐分区。
     * <p>
     * <b>前置（不在本方法内，由 {@code EdqsProcessor} 完成）：</b>
     * <ol>
     *   <li>建好 {@code eventConsumer}（此时尚未订阅分区）；</li>
     *   <li>{@link #init} 建好 {@link #stateConsumer}、{@link #eventsToBackupConsumer}、
     *       {@link #queueStateService}（同样尚未按分区开始干活）。</li>
     * </ol>
     * <p>
     * <b>本方法在收到 PartitionChangeEvent 后的执行顺序：</b>
     * <pre>
     * 首次（queueStateService 尚无任何分区快照）时：
     *   → eventsToBackupConsumer.subscribe(events 全部分区) + launch
     *   → 开始：events 全分区 → 写 state Topic（常驻持续跑，不只启动时）
     *
     * 每次（含首次）再执行 queueStateService.update(本节点负责的分区号)：
     *   对「新增」的每个分区：
     *     a. 先问 backup 消费组当前 committed offset（冻住，避免恢复窗口位点漂移）
     *     b. stateConsumer 订阅对应 state 分区，把历史灌进内存
     *     c. 该 state 分区读完停下 → onStop
     *     d. 再 eventConsumer 订阅同号 events 分区，从 a 记下的 offset seek 接着读
     *   对「移除」的分区：事件侧与状态侧一并卸掉
     *   之后重平衡再来 process：一般不再重新 launch backup（已经在跑），只对增减分区做 b→c→d。
     * </pre>
     * 非首次调用时 backup 消费者已在跑，不再重复 subscribe/launch，只做上面的 update。
     *
     * @param partitions 本节点当前应负责的分区（通常已映射到 state Topic，update 内会再对齐到事件 Topic）
     */
    @Override
    public void process(Set<TopicPartitionInfo> partitions) {
        if (queueStateService.getPartitions().isEmpty()) {
            // 备份消费者始终覆盖全部事件分区，与本节点业务分区分配无关
            Set<TopicPartitionInfo> allPartitions = IntStream.range(0, config.getPartitions())
                    .mapToObj(partition -> TopicPartitionInfo.builder()
                            .topic(config.getEventsTopic())
                            .partition(partition)
                            .build())
                    .collect(Collectors.toSet());
            eventsToBackupConsumer.subscribe(allPartitions);
            eventsToBackupConsumer.launch();
        }
        queueStateService.update(new QueueKey(ServiceType.EDQS), partitions);
    }

    /**
     * Kafka 模式下不在主处理路径写状态。
     * <p>
     * 状态持久化由 {@link #eventsToBackupConsumer} 异步完成；此处保留接口空实现，
     * 以便与 {@link LocalEdqsStateService} 共用同一调用点。
     */
    @Override
    public void save(TenantId tenantId, ObjectType type, String key, EdqsEventType eventType, ToEdqsMsg msg) {
        // do nothing here, backup is done by events consumer
    }

    /**
     * 判断是否已完成至少一轮状态恢复。
     * <p>
     * 当 {@code queueStateService.getPartitionsInProgress()} 非 null 且为空时，
     * 说明已初始化且当前没有「状态恢复中」的分区，于是将 {@link #ready} 置为 true。
     * 一旦为 true 则缓存下来，后续重平衡期间即使短暂出现 in-progress 分区也不再改回 false，
     * 避免查询服务就绪探针抖动。
     *
     * @return 是否已就绪
     */
    @Override
    public boolean isReady() {
        if (ready == null) {
            Set<TopicPartitionInfo> partitionsInProgress = queueStateService.getPartitionsInProgress();
            if (partitionsInProgress != null && partitionsInProgress.isEmpty()) {
                ready = true; // once true - always true, not to change readiness status on each repartitioning
            }
        }
        return ready != null && ready;
    }

    /**
     * 从协议消息中解析租户 ID（MSB/LSB 拼成 UUID）。
     *
     * @param edqsMsg EDQS 协议消息
     * @return 租户 ID
     */
    private TenantId getTenantId(ToEdqsMsg edqsMsg) {
        return TenantId.fromUUID(new UUID(edqsMsg.getTenantIdMSB(), edqsMsg.getTenantIdLSB()));
    }

    /**
     * 停止 state 消费者、备份消费者与 state 生产者。
     * <p>
     * 事件消费者由 {@code EdqsProcessor} 生命周期管理，不在此停止。
     * 注意：此处未调用 {@code queueStateService.stop()}，与当前实现保持一致——
     * 事件侧停止由上层负责，本方法只清理本类额外创建的资源。
     */
    @Override
    public void stop() {
        stateConsumer.stop();
        stateConsumer.awaitStop();
        eventsToBackupConsumer.stop();
        stateProducer.stop();
    }

}
