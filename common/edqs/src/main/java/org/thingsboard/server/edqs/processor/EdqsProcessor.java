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
package org.thingsboard.server.edqs.processor;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.ExceptionUtil;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.server.common.data.ObjectType;
import org.thingsboard.server.common.data.edqs.EdqsEvent;
import org.thingsboard.server.common.data.edqs.EdqsEventType;
import org.thingsboard.server.common.data.edqs.EdqsObject;
import org.thingsboard.server.common.data.edqs.query.EdqsRequest;
import org.thingsboard.server.common.data.edqs.query.EdqsResponse;
import org.thingsboard.server.common.data.edqs.query.QueryResult;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.util.CollectionsUtil;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.edqs.repo.EdqsRepository;
import org.thingsboard.server.edqs.state.EdqsPartitionService;
import org.thingsboard.server.edqs.state.EdqsStateService;
import org.thingsboard.server.edqs.util.EdqsConverter;
import org.thingsboard.server.edqs.util.VersionsStore;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.EdqsEventMsg;
import org.thingsboard.server.gen.transport.TransportProtos.FromEdqsMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdqsMsg;
import org.thingsboard.server.queue.TbQueueHandler;
import org.thingsboard.server.queue.TbQueueResponseTemplate;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.common.consumer.PartitionedQueueConsumerManager;
import org.thingsboard.server.queue.discovery.QueueKey;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;
import org.thingsboard.server.queue.edqs.EdqsComponent;
import org.thingsboard.server.queue.edqs.EdqsConfig;
import org.thingsboard.server.queue.edqs.EdqsConfig.EdqsPartitioningStrategy;
import org.thingsboard.server.queue.edqs.EdqsQueueFactory;
import org.thingsboard.server.queue.util.AfterStartUp;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.thingsboard.server.common.msg.queue.TopicPartitionInfo.withTopic;

/**
 * EDQS 核心处理器：串联「事件消费 → 内存仓库更新 → 查询应答」整条链路。
 * <p>
 * 在 EDQS 组件启用时注册为 Spring Bean（{@link EdqsComponent}）。同时扮演两种角色：
 * <ul>
 *   <li><b>事件侧</b>：创建并持有 events Topic 的 {@link PartitionedQueueConsumerManager}，
 *       将消息交给 {@link #process(ToEdqsMsg, boolean)} 更新 {@link EdqsRepository}；
 *       分区订阅由 {@link EdqsStateService} 在状态恢复完成后驱动；</li>
 *   <li><b>查询侧</b>：实现 {@link TbQueueHandler}，通过 {@link #responseTemplate}
 *       订阅 requests Topic，异步执行实体数据 / 计数查询并回写响应。</li>
 * </ul>
 * <p>
 * <b>启动顺序：</b>
 * {@link #init}（PostConstruct）构建线程池、事件消费者并 {@code stateService.init}；
 * {@link #start}（AfterStartUp）再 launch 请求-响应模板。
 * 分区变更由 {@link #onPartitionsChange} 监听，先交给状态服务恢复，再订阅请求分区，
 * 并在 TENANT 分区策略下清理已迁走租户的内存数据。
 * <p>
 * OOM 时通过 {@link #errorHandler} 清空仓库并关闭 Spring 上下文，避免进程在内存耗尽后继续半死不活运行。
 *
 * @see EdqsStateService
 * @see EdqsRepository
 * @see PartitionedQueueConsumerManager
 */
@EdqsComponent
@Service
@RequiredArgsConstructor
@Slf4j
public class EdqsProcessor implements TbQueueHandler<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>> {

    /**
     * EDQS 队列工厂：创建事件消费者、请求-响应模板及队列管理客户端等。
     */
    private final EdqsQueueFactory queueFactory;

    /**
     * 协议字节与 {@link EdqsObject} 之间的序列化 / 反序列化转换器。
     */
    private final EdqsConverter converter;

    /**
     * 内存中的实体查询仓库。
     * <p>
     * 事件处理后更新索引；查询请求直接读本仓库，不回源主库。
     */
    private final EdqsRepository repository;

    /**
     * EDQS 配置（Topic 名、分区策略、poll 间隔等）。
     */
    private final EdqsConfig config;

    /**
     * 按租户等维度解析逻辑分区号，用于分区迁出时判断哪些租户数据应清理。
     */
    private final EdqsPartitionService partitionService;

    /**
     * Spring 应用上下文。
     * <p>
     * 仅在 OOM 场景下用于异步关闭整个应用。
     */
    private final ConfigurableApplicationContext applicationContext;

    /**
     * 状态服务（Kafka state Topic 或本地 RocksDB）。
     * <p>
     * 负责冷启动 / 分区变更时的状态恢复，并最终驱动事件消费者的分区订阅；
     * 实时事件路径上 {@code backup=true} 时还会调用其 {@code save}。
     */
    private final EdqsStateService stateService;

    /**
     * events Topic 的分区级消费者管理器。
     * <p>
     * 在 {@link #init} 中构建；实际 {@code addPartitions}/{@code update} 由
     * {@link EdqsStateService#process} 在状态就绪后触发，本类不直接改分区。
     */
    private PartitionedQueueConsumerManager<TbProtoQueueMsg<ToEdqsMsg>> eventConsumer;

    /**
     * 请求-响应模板：订阅 requests Topic，将查询消息交给本类 {@link #handle}，再回写 FromEdqsMsg。
     */
    private TbQueueResponseTemplate<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>> responseTemplate;

    /**
     * 事件 / 状态等消费循环使用的线程池（cached）。
     */
    private ExecutorService consumersExecutor;

    /**
     * 消费者管理任务线程池（分区增删等），与 poll 循环隔离。
     */
    private ExecutorService taskExecutor;

    /**
     * 调度线程池：供 {@link PartitionedQueueConsumerManager} 做任务锁重试等延迟调度。
     */
    private ScheduledExecutorService scheduler;

    /**
     * 查询请求执行线程池（ListeningDecorator），供 {@link #handle} 异步提交。
     */
    private ListeningExecutorService requestExecutor;

    /**
     * 事件版本去重存储。
     * <p>
     * 带 version 的事件若不是更新版本则直接丢弃，防止乱序 / 重复投递把仓库状态回退。
     */
    private final VersionsStore versionsStore = new VersionsStore();

    /**
     * 已成功进入仓库处理流程的事件计数（用于周期性 info 日志）。
     */
    private final AtomicInteger counter = new AtomicInteger();

    /**
     * 消费循环未捕获致命错误的回调。
     * <p>
     * 当前仅对 {@link OutOfMemoryError} 做特殊处理：清仓库并关闭应用。
     * 同时注入事件消费者（及 Kafka 状态下的 state 消费者）作为 {@code uncaughtErrorHandler}。
     */
    @Getter
    private Consumer<Throwable> errorHandler;

    /**
     * Bean 初始化：创建线程池、事件消费者、状态服务与请求模板（尚未 launch）。
     * <p>
     * 事件消费者的消息处理固定以 {@code process(msg, true)} 调用，表示来自实时事件路径，
     * 需要按实现决定是否 {@link EdqsStateService#save}。构建完成后立刻
     * {@code stateService.init(eventConsumer)}，把消费者交给状态层编排。
     */
    @PostConstruct
    private void init() {
        consumersExecutor = Executors.newCachedThreadPool(ThingsBoardThreadFactory.forName("edqs-consumer"));
        taskExecutor = ThingsBoardExecutors.newWorkStealingPool(4, "edqs-consumer-task-executor");
        scheduler = ThingsBoardExecutors.newSingleThreadScheduledExecutor("edqs-scheduler");
        requestExecutor = MoreExecutors.listeningDecorator(ThingsBoardExecutors.newWorkStealingPool(12, "edqs-requests"));
        errorHandler = error -> {
            if (error instanceof OutOfMemoryError) {
                log.error("OOM detected, shutting down");
                repository.clear();
                // 独立线程关闭上下文，避免在消费线程栈上直接 destroy 导致死锁或二次异常
                Executors.newSingleThreadExecutor(ThingsBoardThreadFactory.forName("edqs-shutdown"))
                        .execute(applicationContext::close);
            }
        };
        eventConsumer = PartitionedQueueConsumerManager.<TbProtoQueueMsg<ToEdqsMsg>>create()
                .queueKey(new QueueKey(ServiceType.EDQS, config.getEventsTopic()))
                .topic(config.getEventsTopic())
                .pollInterval(config.getPollInterval())
                .msgPackProcessor((msgs, consumer, config) -> {
                    for (TbProtoQueueMsg<ToEdqsMsg> queueMsg : msgs) {
                        if (consumer.isStopped()) {
                            return;
                        }
                        try {
                            ToEdqsMsg msg = queueMsg.getValue();
                            // true：实时事件，可能触发 stateService.save（本地 RocksDB）或空操作（Kafka 备份链）
                            process(msg, true);
                        } catch (Exception t) {
                            log.error("Failed to process message: {}", queueMsg, t);
                        }
                    }
                    consumer.commit();
                })
                .consumerCreator((config, tpi) -> queueFactory.createEdqsEventsConsumer())
                .queueAdmin(queueFactory.getEdqsQueueAdmin())
                .consumerExecutor(consumersExecutor)
                .taskExecutor(taskExecutor)
                .scheduler(scheduler)
                .uncaughtErrorHandler(errorHandler)
                .build();
        stateService.init(eventConsumer);

        responseTemplate = queueFactory.createEdqsResponseTemplate();
    }

    /**
     * 应用启动完成后启动请求-响应消费。
     * <p>
     * 将本类注册为 handler；真正按分区 subscribe 仍由后续 {@link #onPartitionsChange} 完成。
     */
    @AfterStartUp(order = 1)
    public void start() {
        responseTemplate.launch(this);
    }

    /**
     * 处理 EDQS 服务类型的分区变更事件。
     * <p>
     * 流程：
     * <ol>
     *   <li>取出本节点新分区集合，映射到 state Topic 后交给 {@link EdqsStateService#process}
     *       （内部会恢复状态并更新事件消费者分区）；</li>
     *   <li>将同一批分区映射到 requests Topic，让 {@link #responseTemplate} 订阅查询流量
     *       （注：当前会在状态完全就绪前就订阅，代码中有 TODO）；</li>
     *   <li>若有迁出分区且分区策略为 {@link EdqsPartitioningStrategy#TENANT}，
     *       按租户所属分区清理 {@link #repository} 中不再本节点负责的数据；
     *       NONE 策略下出现分区移除会打 warn（预期不应发生）。</li>
     * </ol>
     *
     * @param event 分区发现发出的变更事件；非 EDQS 服务类型直接忽略
     */
    @EventListener
    public void onPartitionsChange(PartitionChangeEvent event) {
        if (event.getServiceType() != ServiceType.EDQS) {
            return;
        }
        try {
            Set<TopicPartitionInfo> newPartitions = event.getNewPartitions().get(new QueueKey(ServiceType.EDQS));

            stateService.process(withTopic(newPartitions, config.getStateTopic()));
            // 事件消费者分区由 stateService 内部更新，此处不必再调 eventConsumer.update
            responseTemplate.subscribe(withTopic(newPartitions, config.getRequestsTopic())); // TODO: we subscribe to partitions before we are ready. implement consumer-per-partition version for request template

            Set<TopicPartitionInfo> oldPartitions = event.getOldPartitions().get(new QueueKey(ServiceType.EDQS));
            if (CollectionsUtil.isNotEmpty(oldPartitions)) {
                Set<Integer> removedPartitions = Sets.difference(oldPartitions, newPartitions).stream()
                        .map(tpi -> tpi.getPartition().orElse(-1)).collect(Collectors.toSet());
                if (removedPartitions.isEmpty()) {
                    return;
                }

                if (config.getPartitioningStrategy() == EdqsPartitioningStrategy.TENANT) {
                    // 租户级分区：迁出分区上的租户数据对本节点已无意义，从内存仓库剔除
                    repository.clearIf(tenantId -> {
                        Integer partition = partitionService.resolvePartition(tenantId, null);
                        return removedPartitions.contains(partition);
                    });
                } else {
                    log.warn("Partitions {} were removed but shouldn't be (due to NONE partitioning strategy)", removedPartitions);
                }
            }
        } catch (Throwable t) {
            log.error("Failed to handle partition change event {}", event, t);
        }
    }

    /**
     * 处理一条 EDQS 查询请求（由 {@link #responseTemplate} 回调）。
     * <p>
     * 在 {@link #requestExecutor} 上异步执行：解析 JSON 请求与租户/客户 ID，
     * 调用 {@link #processRequest}，再包装为 {@link FromEdqsMsg}。
     * 解析失败会向外抛出，由响应模板按错误语义处理。
     *
     * @param queueMsg 携带 {@link ToEdqsMsg}（内含 RequestMsg）的队列消息
     * @return 异步完成的响应消息 Future
     */
    @Override
    public ListenableFuture<TbProtoQueueMsg<FromEdqsMsg>> handle(TbProtoQueueMsg<ToEdqsMsg> queueMsg) {
        ToEdqsMsg toEdqsMsg = queueMsg.getValue();
        return requestExecutor.submit(() -> {
            EdqsRequest request;
            TenantId tenantId;
            CustomerId customerId;
            try {
                request = Objects.requireNonNull(JacksonUtil.fromString(toEdqsMsg.getRequestMsg().getValue(), EdqsRequest.class));
                tenantId = getTenantId(toEdqsMsg);
                customerId = getCustomerId(toEdqsMsg);
            } catch (Exception e) {
                log.error("Failed to parse request msg: {}", toEdqsMsg, e);
                throw e;
            }

            EdqsResponse response = processRequest(tenantId, customerId, request);
            return new TbProtoQueueMsg<>(queueMsg.getKey(), FromEdqsMsg.newBuilder()
                    .setResponseMsg(TransportProtos.EdqsResponseMsg.newBuilder()
                            .setValue(JacksonUtil.toString(response))
                            .build())
                    .build(), queueMsg.getHeaders());
        });
    }

    /**
     * 在本地 {@link EdqsRepository} 上执行实体数据查询或实体计数查询。
     * <p>
     * 业务异常不会抛出，而是写入 {@link EdqsResponse#setError}，保证调用方总能拿到结构化响应。
     *
     * @param tenantId   租户
     * @param customerId 客户（可为 {@code null}，表示租户级查询）
     * @param request    已反序列化的查询请求
     * @return 查询结果或带 error 字段的响应
     */
    private EdqsResponse processRequest(TenantId tenantId, CustomerId customerId, EdqsRequest request) {
        EdqsResponse response = new EdqsResponse();
        try {
            if (request.getEntityDataQuery() != null) {
                PageData<QueryResult> result = repository.findEntityDataByQuery(tenantId, customerId,
                        request.getEntityDataQuery(), false);
                response.setEntityDataQueryResult(result.mapData(QueryResult::toOldEntityData));
            } else if (request.getEntityCountQuery() != null) {
                long result = repository.countEntitiesByQuery(tenantId, customerId, request.getEntityCountQuery(), tenantId.isSysTenantId());
                response.setEntityCountQueryResult(result);
            }
            log.trace("[{}] Request: {}, response: {}", tenantId, request, response);
        } catch (Throwable e) {
            log.error("[{}] Failed to process request: {}", tenantId, request, e);
            response.setError(ExceptionUtil.getMessage(e));
        }
        return response;
    }

    /**
     * 处理一条 EDQS 事件消息：版本校验 → 可选持久化 → 反序列化 → 写入内存仓库。
     * <p>
     * 调用场景：
     * <ul>
     *   <li>{@code backup == true}：来自 events Topic 实时消费；会调用
     *       {@link EdqsStateService#save}（本地实现写 RocksDB，Kafka 实现通常为空）；</li>
     *   <li>{@code backup == false}：来自状态恢复（Kafka state Topic 或 RocksDB 扫描），
     *       不再二次 save，只重建内存索引。</li>
     * </ul>
     * 若事件带 version 且 {@link #versionsStore} 判定非新版本则直接返回；
     * 应带版本却未带的类型会打 warn，但仍继续处理。
     *
     * @param edqsMsg 协议消息；无 {@code eventMsg} 时本方法不做事
     * @param backup  是否走「需要备份到状态存储」的实时事件路径
     */
    public void process(ToEdqsMsg edqsMsg, boolean backup) {
        log.trace("Processing message: {}", edqsMsg);
        if (edqsMsg.hasEventMsg()) {
            EdqsEventMsg eventMsg = edqsMsg.getEventMsg();
            TenantId tenantId = getTenantId(edqsMsg);
            ObjectType objectType = ObjectType.valueOf(eventMsg.getObjectType());
            EdqsEventType eventType = EdqsEventType.valueOf(eventMsg.getEventType());
            String key = eventMsg.getKey();
            Long version = eventMsg.hasVersion() ? eventMsg.getVersion() : null;
            // Kafka/网络可能乱序或重复投递，避免旧事件把内存索引盖成旧数据。这边需要根据版本判断
            if (version != null) {
                if (!versionsStore.isNew(key, version)) {
                    return;
                }
            } else if (!ObjectType.unversionedTypes.contains(objectType)) {
                log.warn("[{}] {} {} doesn't have version", tenantId, objectType, key);
            }
            if (backup) {
                stateService.save(tenantId, objectType, key, eventType, edqsMsg);
            }

            EdqsObject object = converter.deserialize(objectType, eventMsg.getData().toByteArray());
            log.debug("[{}] Processing event [{}] [{}] [{}] [{}]", tenantId, objectType, eventType, key, version);
            int count = counter.incrementAndGet();
            if (count % 100000 == 0) {
                log.info("Processed {} events", count);
            }

            EdqsEvent event = EdqsEvent.builder()
                    .tenantId(tenantId)
                    .objectType(objectType)
                    .eventType(eventType)
                    .object(object)
                    .build();
            repository.processEvent(event);
        }
    }

    /**
     * 从协议消息解析租户 ID（MSB/LSB → UUID）。
     */
    private TenantId getTenantId(ToEdqsMsg edqsMsg) {
        return TenantId.fromUUID(new UUID(edqsMsg.getTenantIdMSB(), edqsMsg.getTenantIdLSB()));
    }

    /**
     * 从协议消息解析客户 ID；MSB/LSB 均为 0 时视为无客户（返回 {@code null}）。
     */
    private CustomerId getCustomerId(ToEdqsMsg edqsMsg) {
        if (edqsMsg.getCustomerIdMSB() != 0 && edqsMsg.getCustomerIdLSB() != 0) {
            return new CustomerId(new UUID(edqsMsg.getCustomerIdMSB(), edqsMsg.getCustomerIdLSB()));
        } else {
            return null;
        }
    }

    /**
     * Bean 销毁：按依赖顺序停止消费者与状态服务，并强制关闭各线程池。
     * <p>
     * 顺序：事件消费者 → 请求模板 → 状态服务 → 线程池 {@code shutdownNow}。
     *
     * @throws InterruptedException 等待事件消费者停止时若线程被中断则抛出
     */
    @PreDestroy
    public void destroy() throws InterruptedException {
        eventConsumer.stop();
        eventConsumer.awaitStop();
        responseTemplate.stop();
        stateService.stop();

        consumersExecutor.shutdownNow();
        taskExecutor.shutdownNow();
        scheduler.shutdownNow();
        requestExecutor.shutdownNow();
    }

}
