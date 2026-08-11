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
package org.thingsboard.server.service.queue;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.common.data.JavaSerDesUtil;
import org.thingsboard.server.common.data.alarm.AlarmInfo;
import org.thingsboard.server.common.data.edqs.ToCoreEdqsMsg;
import org.thingsboard.server.common.data.event.ErrorEvent;
import org.thingsboard.server.common.data.event.Event;
import org.thingsboard.server.common.data.event.LifecycleEvent;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.NotificationRequestId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.notification.rule.trigger.NotificationRuleTrigger;
import org.thingsboard.server.common.data.queue.QueueConfig;
import org.thingsboard.server.common.data.rpc.RpcError;
import org.thingsboard.server.common.msg.MsgType;
import org.thingsboard.server.common.msg.TbActorMsg;
import org.thingsboard.server.common.msg.edqs.EdqsService;
import org.thingsboard.server.common.msg.notification.NotificationRuleProcessor;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.common.msg.rpc.FromDeviceRpcResponse;
import org.thingsboard.server.common.msg.rpc.ToDeviceRpcRequestActorMsg;
import org.thingsboard.server.common.stats.StatsFactory;
import org.thingsboard.server.common.util.KvProtoUtil;
import org.thingsboard.server.common.util.ProtoUtils;
import org.thingsboard.server.dao.resource.ImageCacheKey;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.DeviceStateServiceMsgProto;
import org.thingsboard.server.gen.transport.TransportProtos.ErrorEventProto;
import org.thingsboard.server.gen.transport.TransportProtos.FromDeviceRPCResponseProto;
import org.thingsboard.server.gen.transport.TransportProtos.LifecycleEventProto;
import org.thingsboard.server.gen.transport.TransportProtos.LocalSubscriptionServiceMsgProto;
import org.thingsboard.server.gen.transport.TransportProtos.SubscriptionMgrMsgProto;
import org.thingsboard.server.gen.transport.TransportProtos.TbAlarmDeleteProto;
import org.thingsboard.server.gen.transport.TransportProtos.TbAlarmUpdateProto;
import org.thingsboard.server.gen.transport.TransportProtos.TbAttributeDeleteProto;
import org.thingsboard.server.gen.transport.TransportProtos.TbAttributeUpdateProto;
import org.thingsboard.server.gen.transport.TransportProtos.TbEntitySubEventProto;
import org.thingsboard.server.gen.transport.TransportProtos.TbTimeSeriesDeleteProto;
import org.thingsboard.server.gen.transport.TransportProtos.TbTimeSeriesUpdateProto;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToOtaPackageStateServiceMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import org.thingsboard.server.gen.transport.TransportProtos.TransportToDeviceActorMsg;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.common.consumer.MainQueueConsumerManager;
import org.thingsboard.server.queue.common.consumer.QueueConsumerManager;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.QueueKey;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;
import org.thingsboard.server.queue.provider.TbCoreQueueFactory;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.apiusage.TbApiUsageStateService;
import org.thingsboard.server.service.cf.CalculatedFieldCache;
import org.thingsboard.server.service.notification.NotificationSchedulerService;
import org.thingsboard.server.service.ota.OtaPackageStateService;
import org.thingsboard.server.service.profile.TbAssetProfileCache;
import org.thingsboard.server.service.profile.TbDeviceProfileCache;
import org.thingsboard.server.service.queue.processing.AbstractConsumerService;
import org.thingsboard.server.service.queue.processing.IdMsgPair;
import org.thingsboard.server.service.resource.TbImageService;
import org.thingsboard.server.service.rpc.TbCoreDeviceRpcService;
import org.thingsboard.server.service.ruleengine.RuleEngineCallService;
import org.thingsboard.server.service.security.auth.jwt.settings.JwtSettingsService;

import java.util.Optional;
import org.thingsboard.server.service.state.DeviceStateService;
import org.thingsboard.server.service.subscription.SubscriptionManagerService;
import org.thingsboard.server.service.subscription.TbLocalSubscriptionService;
import org.thingsboard.server.service.subscription.TbSubscriptionUtils;
import org.thingsboard.server.service.sync.vc.GitVersionControlQueueService;
import org.thingsboard.server.service.transport.msg.TransportToDeviceActorMsgWrapper;
import org.thingsboard.server.service.ws.notification.sub.NotificationRequestUpdate;
import org.thingsboard.server.service.ws.notification.sub.NotificationUpdate;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TB Core 节点的默认队列消费服务实现。
 * <p>
 * 本类是 Core 服务侧「从消息队列取消息并分发到业务组件」的中枢：在分区发现就绪后订阅本节点负责的 Topic 分区，
 * 将各类协议消息路由到设备状态、订阅管理、RPC、OTA、用量统计、通知调度、EDQS 等服务。
 * <p>
 * 消费通道一览：
 * <ul>
 *   <li>{@link #mainConsumer}：主通道，消费 {@link ToCoreMsg}
 *       （订阅管理、设备 Actor、设备在线/活跃、通知调度、错误/生命周期事件等），
 *       由 {@link MainQueueConsumerManager} 管理，支持按分区创建消费者；</li>
 *   <li>通知通道：继承自 {@link AbstractConsumerService}，消费 {@link ToCoreNotificationMsg}
 *       （本机订阅推送、Core 启动协调、设备 RPC 响应、队列变更、版本控制响应、资源缓存失效等）；</li>
 *   <li>{@link #usageStatsConsumer}：API / 用量统计消息；</li>
 *   <li>{@link #firmwareStatesConsumer}：OTA 包状态更新（带限速，避免短时间打爆下游）。</li>
 * </ul>
 * 分区变更时通过 {@link #onTbApplicationEvent(PartitionChangeEvent)} 更新主消费与用量统计订阅；
 * 一批消息采用「提交处理 + 超时等待回调」模式，超时后仍会 {@code commit}，由上层重试/幂等策略兜底。
 *
 * @see TbCoreConsumerService
 * @see AbstractConsumerService
 * @see MainQueueConsumerManager
 */
@Service
@Slf4j
@TbCoreComponent
public class DefaultTbCoreConsumerService extends AbstractConsumerService<ToCoreNotificationMsg> implements TbCoreConsumerService {

    /** 主队列 / 通知队列的 poll 间隔（毫秒），对应配置 {@code queue.core.poll-interval} */
    @Value("${queue.core.poll-interval}")
    private long pollInterval;

    /**
     * 单批消息处理的最长等待时间（毫秒）。
     * <p>
     * 批次内每条消息通过 {@link TbPackCallback} 在成功或失败时减少待处理计数；
     * 若在超时前未全部完成，会取消尚未提交完的提交任务并记录超时/失败明细，然后仍提交 offset。
     */
    @Value("${queue.core.pack-processing-timeout}")
    private long packProcessingTimeout;

    /**
     * 主通道是否「每分区一个消费者」。
     * <p>
     * {@code true} 时隔离性更好、分区故障不易互相拖累；{@code false} 时单消费者订阅全部分区，资源占用更少。
     */
    @Value("${queue.core.consumer-per-partition:true}")
    private boolean consumerPerPartition;

    /** 是否启用本服务内的消费统计（路由类型计数等），并由定时任务打印 */
    @Value("${queue.core.stats.enabled:false}")
    private boolean statsEnabled;

    /**
     * OTA 状态消息整批处理的时间预算窗口（毫秒）。
     * <p>
     * 与 {@link #firmwarePackSize} 一起决定「单条成功处理后建议休眠多久」，用于平滑推送节奏。
     */
    @Value("${queue.core.ota.pack-interval-ms:60000}")
    private long firmwarePackInterval;

    /**
     * 在 {@link #firmwarePackInterval} 时间窗内期望处理的 OTA 记录条数上限（用于计算单条间隔）。
     */
    @Value("${queue.core.ota.pack-size:100}")
    private int firmwarePackSize;

    /** 设备连接/活跃/断开/不活跃等状态服务 */
    private final DeviceStateService stateService;
    /** 租户 API 用量与配额相关统计处理 */
    private final TbApiUsageStateService statsService;
    /** 本机 WebSocket / 本地订阅推送服务 */
    private final TbLocalSubscriptionService localSubscriptionService;
    /** 跨节点订阅管理（实体订阅、遥测/属性/告警变更分发） */
    private final SubscriptionManagerService subscriptionManagerService;
    /** Core 侧设备 RPC：向设备 Actor 转发请求、处理规则引擎侧响应 */
    private final TbCoreDeviceRpcService tbCoreDeviceRpcService;
    /** OTA 包状态机处理 */
    private final OtaPackageStateService firmwareStateService;
    /** Git 版本控制队列响应处理 */
    private final GitVersionControlQueueService vcQueueService;
    /** 通知请求调度（按时间触发发送） */
    private final NotificationSchedulerService notificationSchedulerService;
    /** 通知规则触发器处理 */
    private final NotificationRuleProcessor notificationRuleProcessor;
    /** 创建各类 Core 队列消费者的工厂 */
    private final TbCoreQueueFactory queueFactory;
    /** 图片等资源 ETag / 缓存失效 */
    private final TbImageService imageService;
    /** REST API 经规则引擎调用后的响应回调 */
    private final RuleEngineCallService ruleEngineCallService;
    /** EDQS（实体数据查询服务）系统消息处理 */
    private final EdqsService edqsService;
    /** 本服务消费路径上的统计计数器 */
    private final TbCoreConsumerStats stats;

    /**
     * 主消息消费者管理器：订阅 TB_CORE 主 Topic，按分区消费 {@link ToCoreMsg}。
     * <p>
     * 分区集合由 {@link PartitionChangeEvent#getCorePartitions()} 驱动更新；
     * 消息批处理入口为 {@link #processMsgs}。
     */
    private MainQueueConsumerManager<TbProtoQueueMsg<ToCoreMsg>, QueueConfig> mainConsumer;

    /**
     * 用量统计消费者：Topic 随 Core 分区变化而重映射（同分区号、不同 Topic 名）。
     */
    private QueueConsumerManager<TbProtoQueueMsg<ToUsageStatsServiceMsg>> usageStatsConsumer;

    /**
     * OTA 包状态消费者：启动时全局订阅并 launch，不随分区事件在本类中反复 update
     * （其分区语义由底层消费者/工厂决定）。
     */
    private QueueConsumerManager<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>> firmwareStatesConsumer;

    /**
     * 设备活跃类事件的单线程执行器。
     * <p>
     * 连接、活跃、断开、不活跃、不活跃超时更新等消息先入队到此线程再调 {@link DeviceStateService}，
     * 避免在消费线程上阻塞，并保证同类状态更新串行，降低竞态。
     */
    private volatile ListeningExecutorService deviceActivityEventsExecutor;

    /**
     * 组装 Core 消费所需的全部协作服务，并初始化父类中的分区/档案缓存等基础设施。
     * <p>
     * 消费者实例本身在 {@link #init()} 中创建，构造阶段只保存依赖。
     */
    public DefaultTbCoreConsumerService(TbCoreQueueFactory tbCoreQueueFactory,
                                        ActorSystemContext actorContext,
                                        DeviceStateService stateService,
                                        TbLocalSubscriptionService localSubscriptionService,
                                        SubscriptionManagerService subscriptionManagerService,
                                        TbCoreDeviceRpcService tbCoreDeviceRpcService,
                                        StatsFactory statsFactory,
                                        TbDeviceProfileCache deviceProfileCache,
                                        TbAssetProfileCache assetProfileCache,
                                        TbApiUsageStateService statsService,
                                        TbTenantProfileCache tenantProfileCache,
                                        TbApiUsageStateService apiUsageStateService,
                                        OtaPackageStateService firmwareStateService,
                                        GitVersionControlQueueService vcQueueService,
                                        PartitionService partitionService,
                                        ApplicationEventPublisher eventPublisher,
                                        Optional<JwtSettingsService> jwtSettingsService,
                                        NotificationSchedulerService notificationSchedulerService,
                                        NotificationRuleProcessor notificationRuleProcessor,
                                        TbImageService imageService,
                                        RuleEngineCallService ruleEngineCallService,
                                        CalculatedFieldCache calculatedFieldCache,
                                        EdqsService edqsService) {
        super(actorContext, tenantProfileCache, deviceProfileCache, assetProfileCache, calculatedFieldCache, apiUsageStateService, partitionService,
                eventPublisher, jwtSettingsService);
        this.stateService = stateService;
        this.localSubscriptionService = localSubscriptionService;
        this.subscriptionManagerService = subscriptionManagerService;
        this.tbCoreDeviceRpcService = tbCoreDeviceRpcService;
        this.stats = new TbCoreConsumerStats(statsFactory);
        this.statsService = statsService;
        this.firmwareStateService = firmwareStateService;
        this.vcQueueService = vcQueueService;
        this.notificationSchedulerService = notificationSchedulerService;
        this.notificationRuleProcessor = notificationRuleProcessor;
        this.imageService = imageService;
        this.ruleEngineCallService = ruleEngineCallService;
        this.queueFactory = tbCoreQueueFactory;
        this.edqsService = edqsService;
    }

    /**
     * Bean 初始化：调用父类 {@code init("tb-core")} 准备公共线程池与通知消费者，
     * 再创建设备活跃事件执行器以及主通道、用量统计、OTA 三类消费者管理器。
     * <p>
     * 注意：此处只 build / 配置消费者，真正 subscribe / launch 在
     * {@link #startConsumers()}（以及父类通知消费者启动逻辑）中完成。
     */
    @PostConstruct
    public void init() {
        super.init("tb-core");
        this.deviceActivityEventsExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(ThingsBoardThreadFactory.forName("tb-core-device-activity-events-executor")));

        this.mainConsumer = MainQueueConsumerManager.<TbProtoQueueMsg<ToCoreMsg>, QueueConfig>builder()
                .queueKey(new QueueKey(ServiceType.TB_CORE))
                .config(QueueConfig.of(consumerPerPartition, pollInterval))
                .msgPackProcessor(this::processMsgs)
                .consumerCreator((config, tpi) -> queueFactory.createToCoreMsgConsumer())
                .consumerExecutor(consumersExecutor)
                .scheduler(scheduler)
                .taskExecutor(mgmtExecutor)
                .build();
        this.usageStatsConsumer = QueueConsumerManager.<TbProtoQueueMsg<ToUsageStatsServiceMsg>>builder()
                .name("TB Usage Stats")
                .msgPackProcessor(this::processUsageStatsMsg)
                .pollInterval(pollInterval)
                .consumerCreator(queueFactory::createToUsageStatsServiceMsgConsumer)
                .consumerExecutor(consumersExecutor)
                .threadPrefix("usage-stats")
                .build();
        this.firmwareStatesConsumer = QueueConsumerManager.<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>>builder()
                .name("TB Ota Package States")
                .msgPackProcessor(this::processFirmwareMsgs)
                .pollInterval(pollInterval)
                .consumerCreator(queueFactory::createToOtaPackageStateServiceMsgConsumer)
                .consumerExecutor(consumersExecutor)
                .threadPrefix("firmware")
                .build();
    }

    /**
     * Bean 销毁：先走父类销毁（停公共资源），再强制关闭设备活跃事件线程池。
     */
    @PreDestroy
    public void destroy() {
        super.destroy();
        if (deviceActivityEventsExecutor != null) {
            deviceActivityEventsExecutor.shutdownNow();
        }
    }

    /**
     * 启动各消费通道。
     * <p>
     * 顺序：父类启动通知消费者 → OTA 订阅并 launch → 用量统计 launch。
     * 主通道 {@link #mainConsumer} 不在此处 launch 空跑，而是等分区事件
     * {@link #onTbApplicationEvent} 带上分区后再 {@code update}，由
     * {@link MainQueueConsumerManager} 按分区创建并启动消费者。
     */
    @Override
    protected void startConsumers() {
        super.startConsumers();
        firmwareStatesConsumer.subscribe();
        firmwareStatesConsumer.launch();
        usageStatsConsumer.launch();
    }

    /**
     * 本节点 Core 分区集合发生变化时的回调。
     * <p>
     * 将最新 Core 分区交给 {@link #mainConsumer}；用量统计 Topic 则按「相同分区信息、
     * 替换为 usage-stats Topic」重新 subscribe，保证与主分区归属一致。
     *
     * @param event 分区变更事件，其中 {@link PartitionChangeEvent#getCorePartitions()} 为本节点应负责的 Core 分区
     */
    @Override
    protected void onTbApplicationEvent(PartitionChangeEvent event) {
        log.debug("Subscribing to partitions: {}", event.getCorePartitions());
        mainConsumer.update(event.getCorePartitions());
        usageStatsConsumer.subscribe(event.getCorePartitions()
                .stream()
                .map(tpi -> tpi.newByTopic(usageStatsConsumer.getConsumer().getTopic()))
                .collect(Collectors.toSet()));
    }

    /**
     * 主通道一批 {@link ToCoreMsg} 的处理入口。
     * <p>
     * 处理模型：
     * <ol>
     *   <li>为每条消息生成随机 UUID，放入 pending 映射；</li>
     *   <li>在 {@code consumersExecutor} 中串行提交每条消息的路由逻辑，成功/失败通过
     *       {@link TbPackCallback} 更新 {@link TbPackProcessingContext}；</li>
     *   <li>当前线程用 {@link CountDownLatch} 等待整批完成，最长 {@link #packProcessingTimeout}；</li>
     *   <li>超时则尝试取消提交 Future，打印仍未 ack 或已失败的消息；</li>
     *   <li>无论是否超时，最后对当前消费者 {@code commit} offset。</li>
     * </ol>
     * 路由目标包括：订阅管理、设备 Actor、设备状态（含连接/活跃等）、设备 Actor 通知（含 RPC）、
     * 通知调度、错误事件、生命周期事件等。
     *
     * @param msgs     本批协议消息，由 {@link MainQueueConsumerManager} 的 poll 循环传入
     * @param consumer 拉取到本批消息的底层消费者，用于 commit
     * @param config   当前队列配置（本方法未直接使用，满足处理器签名）
     * @throws Exception 等待超时 latch 被中断等场景可能抛出
     */
    private void processMsgs(List<TbProtoQueueMsg<ToCoreMsg>> msgs, TbQueueConsumer<TbProtoQueueMsg<ToCoreMsg>> consumer, QueueConfig config) throws Exception {
        List<IdMsgPair<ToCoreMsg>> orderedMsgList = msgs.stream().map(msg -> new IdMsgPair<>(UUID.randomUUID(), msg)).toList();
        ConcurrentMap<UUID, TbProtoQueueMsg<ToCoreMsg>> pendingMap = orderedMsgList.stream().collect(
                Collectors.toConcurrentMap(IdMsgPair::getUuid, IdMsgPair::getMsg));
        CountDownLatch processingTimeoutLatch = new CountDownLatch(1);
        TbPackProcessingContext<TbProtoQueueMsg<ToCoreMsg>> ctx = new TbPackProcessingContext<>(
                processingTimeoutLatch, pendingMap, new ConcurrentHashMap<>());
        PendingMsgHolder<ToCoreMsg> pendingMsgHolder = new PendingMsgHolder<>();
        Future<?> packSubmitFuture = consumersExecutor.submit(() -> {
            orderedMsgList.forEach((element) -> {
                UUID id = element.getUuid();
                TbProtoQueueMsg<ToCoreMsg> msg = element.getMsg();
                log.trace("[{}] Creating main callback for message: {}", id, msg.getValue());
                TbCallback callback = new TbPackCallback<>(id, ctx);
                try {
                    ToCoreMsg toCoreMsg = msg.getValue();
                    pendingMsgHolder.setMsg(toCoreMsg);
                    if (toCoreMsg.hasToSubscriptionMgrMsg()) {
                        log.trace("[{}] Forwarding message to subscription manager service {}", id, toCoreMsg.getToSubscriptionMgrMsg());
                        forwardToSubMgrService(toCoreMsg.getToSubscriptionMgrMsg(), callback);
                    } else if (toCoreMsg.hasToDeviceActorMsg()) {
                        log.trace("[{}] Forwarding message to device actor {}", id, toCoreMsg.getToDeviceActorMsg());
                        forwardToDeviceActor(toCoreMsg.getToDeviceActorMsg(), callback);
                    } else if (toCoreMsg.hasDeviceStateServiceMsg()) {
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceStateServiceMsg());
                        forwardToStateService(toCoreMsg.getDeviceStateServiceMsg(), callback);
                    } else if (toCoreMsg.hasDeviceConnectMsg()) {
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceConnectMsg());
                        forwardToStateService(toCoreMsg.getDeviceConnectMsg(), callback);
                    } else if (toCoreMsg.hasDeviceActivityMsg()) {
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceActivityMsg());
                        forwardToStateService(toCoreMsg.getDeviceActivityMsg(), callback);
                    } else if (toCoreMsg.hasDeviceDisconnectMsg()) {
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceDisconnectMsg());
                        forwardToStateService(toCoreMsg.getDeviceDisconnectMsg(), callback);
                    } else if (toCoreMsg.hasDeviceInactivityMsg()) {
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceInactivityMsg());
                        forwardToStateService(toCoreMsg.getDeviceInactivityMsg(), callback);
                    } else if (toCoreMsg.hasDeviceInactivityTimeoutUpdateMsg()) {
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceInactivityTimeoutUpdateMsg());
                        forwardToStateService(toCoreMsg.getDeviceInactivityTimeoutUpdateMsg(), callback);
                    } else if (toCoreMsg.hasToDeviceActorNotification()) {
                        TbActorMsg actorMsg = ProtoUtils.fromProto(toCoreMsg.getToDeviceActorNotification());
                        if (actorMsg != null) {
                            if (actorMsg.getMsgType().equals(MsgType.DEVICE_RPC_REQUEST_TO_DEVICE_ACTOR_MSG)) {
                                tbCoreDeviceRpcService.forwardRpcRequestToDeviceActor((ToDeviceRpcRequestActorMsg) actorMsg);
                            } else {
                                log.trace("[{}] Forwarding message to App Actor {}", id, actorMsg);
                                actorContext.tell(actorMsg);
                            }
                        }
                        callback.onSuccess();
                    } else if (toCoreMsg.hasNotificationSchedulerServiceMsg()) {
                        TransportProtos.NotificationSchedulerServiceMsg notificationSchedulerServiceMsg = toCoreMsg.getNotificationSchedulerServiceMsg();
                        log.trace("[{}] Forwarding message to notification scheduler service {}", id, toCoreMsg.getNotificationSchedulerServiceMsg());
                        forwardToNotificationSchedulerService(notificationSchedulerServiceMsg, callback);
                    } else if (toCoreMsg.hasErrorEventMsg()) {
                        forwardToEventService(toCoreMsg.getErrorEventMsg(), callback);
                    } else if (toCoreMsg.hasLifecycleEventMsg()) {
                        forwardToEventService(toCoreMsg.getLifecycleEventMsg(), callback);
                    }
                } catch (Throwable e) {
                    log.warn("[{}] Failed to process message: {}", id, msg, e);
                    callback.onFailure(e);
                }
            });
        });
        if (!processingTimeoutLatch.await(packProcessingTimeout, TimeUnit.MILLISECONDS)) {
            if (!packSubmitFuture.isDone()) {
                packSubmitFuture.cancel(true);
                log.info("Timeout to process message: {}", pendingMsgHolder.getMsg());
            }
            if (log.isDebugEnabled()) {
                ctx.getAckMap().forEach((id, msg) -> log.debug("[{}] Timeout to process message: {}", id, msg.getValue()));
            }
            ctx.getFailedMap().forEach((id, msg) -> log.warn("[{}] Failed to process message: {}", id, msg.getValue()));
        }
        consumer.commit();
    }

    /**
     * @return 固定为 {@link ServiceType#TB_CORE}，供父类分区与通知逻辑识别服务类型
     */
    @Override
    protected ServiceType getServiceType() {
        return ServiceType.TB_CORE;
    }

    /**
     * @return 通知队列 poll 间隔，与主队列共用 {@link #pollInterval}
     */
    @Override
    protected long getNotificationPollDuration() {
        return pollInterval;
    }

    /**
     * @return 通知批处理超时，与主队列共用 {@link #packProcessingTimeout}
     */
    @Override
    protected long getNotificationPackProcessingTimeout() {
        return packProcessingTimeout;
    }

    /**
     * 管理任务线程池大小：至少 4，且不小于 CPU 核数。
     * <p>
     * 该池由父类创建，并作为 {@link #mainConsumer} 的 {@code taskExecutor}，
     * 用于处理配置/分区等管理任务，而非消息 poll 循环本身。
     */
    @Override
    protected int getMgmtThreadPoolSize() {
        return Math.max(Runtime.getRuntime().availableProcessors(), 4);
    }

    /**
     * 创建 Core 通知队列消费者，供父类通知消费循环使用。
     */
    @Override
    protected TbQueueConsumer<TbProtoQueueMsg<ToCoreNotificationMsg>> createNotificationsConsumer() {
        return queueFactory.createToCoreNotificationsMsgConsumer();
    }

    /**
     * 处理单条 {@link ToCoreNotificationMsg}（由父类通知消费框架调用）。
     * <p>
     * 按 oneof 字段路由到：本机订阅服务、Core 启动协调、设备 RPC 响应、规则引擎 REST 回调、
     * 组件生命周期、队列增删、版本控制响应、订阅管理、通知规则、资源缓存失效、EDQS 等。
     * 在 {@link #statsEnabled} 时记录通知统计。
     *
     * @param id       父类为该消息分配的处理 ID（日志关联）
     * @param msg      队列封装后的协议消息
     * @param callback 处理完成回调；多数分支在转发结束后调用成功/失败
     */
    @Override
    protected void handleNotification(UUID id, TbProtoQueueMsg<ToCoreNotificationMsg> msg, TbCallback callback) {
        ToCoreNotificationMsg toCoreNotification = msg.getValue();
        if (toCoreNotification.hasToLocalSubscriptionServiceMsg()) {
            log.trace("[{}] Forwarding message to local subscription service {}", id, toCoreNotification.getToLocalSubscriptionServiceMsg());
            forwardToLocalSubMgrService(toCoreNotification.getToLocalSubscriptionServiceMsg(), callback);
        } else if (toCoreNotification.hasCoreStartupMsg()) {
            log.trace("[{}] Forwarding message to local subscription service {}", id, toCoreNotification.getCoreStartupMsg());
            forwardCoreStartupMsg(toCoreNotification.getCoreStartupMsg(), callback);
        } else if (toCoreNotification.hasFromDeviceRpcResponse()) {
            log.trace("[{}] Forwarding message to RPC service {}", id, toCoreNotification.getFromDeviceRpcResponse());
            forwardToCoreRpcService(toCoreNotification.getFromDeviceRpcResponse(), callback);
        } else if (toCoreNotification.hasRestApiCallResponseMsg()) {
            log.trace("[{}] Forwarding message to RuleEngineCallService service {}", id, toCoreNotification.getRestApiCallResponseMsg());
            forwardToRuleEngineCallService(toCoreNotification.getRestApiCallResponseMsg(), callback);
        } else if (toCoreNotification.hasComponentLifecycle()) {
            handleComponentLifecycleMsg(id, ProtoUtils.fromProto(toCoreNotification.getComponentLifecycle()));
            callback.onSuccess();
        } else if (toCoreNotification.getQueueUpdateMsgsCount() > 0) {
            partitionService.updateQueues(toCoreNotification.getQueueUpdateMsgsList());
            callback.onSuccess();
        } else if (toCoreNotification.getQueueDeleteMsgsCount() > 0) {
            partitionService.removeQueues(toCoreNotification.getQueueDeleteMsgsList());
            callback.onSuccess();
        } else if (toCoreNotification.hasVcResponseMsg()) {
            vcQueueService.processResponse(toCoreNotification.getVcResponseMsg());
            callback.onSuccess();
        } else if (toCoreNotification.hasToSubscriptionMgrMsg()) {
            forwardToSubMgrService(toCoreNotification.getToSubscriptionMgrMsg(), callback);
        } else if (toCoreNotification.hasNotificationRuleProcessorMsg()) {
            NotificationRuleTrigger notificationRuleTrigger =
                    JavaSerDesUtil.decode(toCoreNotification.getNotificationRuleProcessorMsg().getTrigger().toByteArray());
            notificationRuleProcessor.process(notificationRuleTrigger);
            callback.onSuccess();
        } else if (toCoreNotification.hasResourceCacheInvalidateMsg()) {
            forwardToResourceService(toCoreNotification.getResourceCacheInvalidateMsg(), callback);
        } else if (toCoreNotification.hasToEdqsCoreServiceMsg()) {
            edqsService.processSystemMsg(JacksonUtil.fromBytes(toCoreNotification.getToEdqsCoreServiceMsg().getValue().toByteArray(), ToCoreEdqsMsg.class));
            callback.onSuccess();
        }
        if (statsEnabled) {
            stats.log(toCoreNotification);
        }
    }

    /**
     * 处理一批用量统计消息：为每条消息建立 pack 回调上下文，调用 {@link #handleUsageStats}，
     * 等待整批完成或超时后 commit。
     *
     * @param msgs     用量统计协议消息列表
     * @param consumer 对应队列消费者
     * @throws Exception latch 等待被中断时抛出
     */
    private void processUsageStatsMsg(List<TbProtoQueueMsg<ToUsageStatsServiceMsg>> msgs, TbQueueConsumer<TbProtoQueueMsg<ToUsageStatsServiceMsg>> consumer) throws Exception {
        ConcurrentMap<UUID, TbProtoQueueMsg<ToUsageStatsServiceMsg>> pendingMap = msgs.stream().collect(
                Collectors.toConcurrentMap(s -> UUID.randomUUID(), Function.identity()));
        CountDownLatch processingTimeoutLatch = new CountDownLatch(1);
        TbPackProcessingContext<TbProtoQueueMsg<ToUsageStatsServiceMsg>> ctx = new TbPackProcessingContext<>(
                processingTimeoutLatch, pendingMap, new ConcurrentHashMap<>());
        pendingMap.forEach((id, msg) -> {
            log.trace("[{}] Creating usage stats callback for message: {}", id, msg.getValue());
            TbCallback callback = new TbPackCallback<>(id, ctx);
            try {
                handleUsageStats(msg, callback);
            } catch (Throwable e) {
                log.warn("[{}] Failed to process usage stats: {}", id, msg, e);
                callback.onFailure(e);
            }
        });
        if (!processingTimeoutLatch.await(getNotificationPackProcessingTimeout(), TimeUnit.MILLISECONDS)) {
            ctx.getAckMap().forEach((id, msg) -> log.warn("[{}] Timeout to process usage stats: {}", id, msg.getValue()));
            ctx.getFailedMap().forEach((id, msg) -> log.warn("[{}] Failed to process usage stats: {}", id, msg.getValue()));
        }
        consumer.commit();

    }

    /**
     * 处理一批 OTA 包状态消息，并按配置做节流。
     * <p>
     * 单条「理想间隔」为 {@code firmwarePackInterval / firmwarePackSize}。
     * 仅当 {@link #handleOtaPackageUpdates} 返回成功更新时，才会按剩余时间预算 sleep，
     * 避免无效消息也占用限速额度。线程中断时直接结束本批循环（不抛出让调用方感知中断意图）。
     *
     * @param msgs     OTA 状态消息列表
     * @param consumer 对应队列消费者，批末 commit
     */
    private void processFirmwareMsgs(List<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>> msgs, TbQueueConsumer<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>> consumer) {
        long maxProcessingTimeoutPerRecord = firmwarePackInterval / firmwarePackSize;
        long timeToSleep = maxProcessingTimeoutPerRecord;
        for (TbProtoQueueMsg<ToOtaPackageStateServiceMsg> msg : msgs) {
            try {
                long startTime = System.currentTimeMillis();
                boolean isSuccessUpdate = handleOtaPackageUpdates(msg);
                long endTime = System.currentTimeMillis();
                long spentTime = endTime - startTime;
                timeToSleep = timeToSleep - spentTime;
                if (isSuccessUpdate) {
                    if (timeToSleep > 0) {
                        log.debug("Spent time per record is: [{}]!", spentTime);
                        Thread.sleep(timeToSleep);
                        timeToSleep = 0;
                    }
                    timeToSleep += maxProcessingTimeoutPerRecord;
                }
            } catch (InterruptedException e) {
                return;
            } catch (Throwable e) {
                log.warn("Failed to process firmware update msg: {}", msg, e);
            }
        }
        consumer.commit();
    }

    /**
     * 将用量统计消息交给 {@link TbApiUsageStateService}，由其在适当时机触发 callback。
     */
    private void handleUsageStats(TbProtoQueueMsg<ToUsageStatsServiceMsg> msg, TbCallback callback) {
        statsService.process(msg, callback);
    }

    /**
     * 将 OTA 状态协议消息交给 {@link OtaPackageStateService}。
     *
     * @return 是否发生了成功的状态更新（用于限速逻辑）
     */
    private boolean handleOtaPackageUpdates(TbProtoQueueMsg<ToOtaPackageStateServiceMsg> msg) {
        return firmwareStateService.process(msg.getValue());
    }

    /**
     * 将规则引擎侧返回的设备 RPC 响应转成 {@link FromDeviceRpcResponse}，
     * 交给 {@link TbCoreDeviceRpcService} 继续完成调用方等待逻辑。
     */
    private void forwardToCoreRpcService(FromDeviceRPCResponseProto proto, TbCallback callback) {
        RpcError error = proto.getError() > 0 ? RpcError.values()[proto.getError()] : null;
        FromDeviceRpcResponse response = new FromDeviceRpcResponse(new UUID(proto.getRequestIdMSB(), proto.getRequestIdLSB())
                , proto.getResponse(), error);
        tbCoreDeviceRpcService.processRpcResponseFromRuleEngine(response);
        callback.onSuccess();
    }

    /**
     * 定时打印并重置消费统计，间隔由 {@code queue.core.stats.print-interval-ms} 控制。
     * 仅在 {@link #statsEnabled} 为 true 时实际输出。
     */
    @Scheduled(fixedDelayString = "${queue.core.stats.print-interval-ms}")
    public void printStats() {
        if (statsEnabled) {
            stats.printStats();
            stats.reset();
        }
    }

    /**
     * 将「面向本机订阅服务」的通知消息分发给 {@link TbLocalSubscriptionService}。
     * <p>
     * 支持：订阅事件回调、时序/属性/告警更新、通知更新；若干旧版 subUpdate 字段仅 ack 成功、不做处理。
     * 无法识别的消息走 {@link #throwNotHandled}。
     *
     * @param msg      本机订阅服务协议体
     * @param callback 完成回调
     */
    private void forwardToLocalSubMgrService(LocalSubscriptionServiceMsgProto msg, TbCallback callback) {
        if (msg.hasSubEventCallback()) {
            localSubscriptionService.onSubEventCallback(msg.getSubEventCallback(), callback);
        } else if (msg.hasTsUpdate()) {
            localSubscriptionService.onTimeSeriesUpdate(msg.getTsUpdate(), callback);
        } else if (msg.hasAttrUpdate()) {
            localSubscriptionService.onAttributesUpdate(msg.getAttrUpdate(), callback);
        } else if (msg.hasAlarmUpdate()) {
            localSubscriptionService.onAlarmUpdate(msg.getAlarmUpdate(), callback);
        } else if (msg.hasNotificationsUpdate()) {
            localSubscriptionService.onNotificationUpdate(msg.getNotificationsUpdate(), callback);
        } else if (msg.hasSubUpdate() || msg.hasAlarmSubUpdate() || msg.hasNotificationsSubUpdate()) {
            //OLD CODE -> Do NOTHING.
            callback.onSuccess();
        } else {
            throwNotHandled(msg, callback);
        }
    }

    /**
     * 处理其它 Core 节点启动时发来的协调消息：刷新本机订阅与分区相关状态。
     * <p>
     * 多 Core 实例场景下，新节点启动后需要让已有节点重新对齐订阅分发目标。
     *
     * @param coreStartupMsg 含 serviceId 与分区列表
     * @param callback       完成回调
     */
    private void forwardCoreStartupMsg(TransportProtos.CoreStartupMsg coreStartupMsg, TbCallback callback) {
        log.info("[{}] Processing core startup with partitions: {}", coreStartupMsg.getServiceId(), coreStartupMsg.getPartitionsList());
        localSubscriptionService.onCoreStartupMsg(coreStartupMsg);
        callback.onSuccess();
    }

    /**
     * 根据资源缓存失效消息，驱逐对应图片的 ETag 缓存（租户图片或公开图片）。
     *
     * @param msg      失效键列表协议
     * @param callback 完成回调
     */
    private void forwardToResourceService(TransportProtos.ResourceCacheInvalidateMsg msg, TbCallback callback) {
        var tenantId = TenantId.fromUUID(new UUID(msg.getTenantIdMSB(), msg.getTenantIdLSB()));
        msg.getKeysList().stream().map(cacheKeyProto -> {
            if (cacheKeyProto.hasResourceKey()) {
                return ImageCacheKey.forImage(tenantId, cacheKeyProto.getResourceKey());
            } else {
                return ImageCacheKey.forPublicImage(cacheKeyProto.getPublicResourceKey());
            }
        }).forEach(imageService::evictETags);
        callback.onSuccess();
    }

    /**
     * 将订阅管理协议消息路由到 {@link SubscriptionManagerService}（或个别分支到本机订阅服务）。
     * <p>
     * 主要分支：
     * <ul>
     *   <li>实体订阅事件 {@code subEvent}；</li>
     *   <li>时序/属性更新与删除、告警更新与删除；</li>
     *   <li>通知更新、通知请求更新；</li>
     *   <li>若干已废弃的 telemetry/alarm/notifications 订阅字段：仅成功回调，避免旧消息打挂消费。</li>
     * </ul>
     * 属性删除需兼容仍携带废弃字段 {@code notifyDevice} 的旧消息。
     *
     * @param msg      订阅管理协议体
     * @param callback 完成回调
     */
    private void forwardToSubMgrService(SubscriptionMgrMsgProto msg, TbCallback callback) {
        if (msg.hasSubEvent()) {
            TbEntitySubEventProto subEvent = msg.getSubEvent();
            subscriptionManagerService.onSubEvent(subEvent.getServiceId(), TbSubscriptionUtils.fromProto(subEvent), callback);
        } else if (msg.hasTelemetrySub()) {
            callback.onSuccess();
            // Deprecated, for removal; Left intentionally to avoid throwNotHandled
        } else if (msg.hasAlarmSub()) {
            callback.onSuccess();
            // Deprecated, for removal; Left intentionally to avoid throwNotHandled
        } else if (msg.hasNotificationsSub()) {
            callback.onSuccess();
            // Deprecated, for removal; Left intentionally to avoid throwNotHandled
        } else if (msg.hasNotificationsCountSub()) {
            callback.onSuccess();
            // Deprecated, for removal; Left intentionally to avoid throwNotHandled
        } else if (msg.hasSubClose()) {
            callback.onSuccess();
            // Deprecated, for removal; Left intentionally to avoid throwNotHandled
        } else if (msg.hasTsUpdate()) {
            TbTimeSeriesUpdateProto proto = msg.getTsUpdate();
            long tenantIdMSB = proto.getTenantIdMSB();
            long tenantIdLSB = proto.getTenantIdLSB();
            subscriptionManagerService.onTimeSeriesUpdate(
                    toTenantId(tenantIdMSB, tenantIdLSB),
                    TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                    KvProtoUtil.fromTsKvProtoList(proto.getDataList()), callback);
        } else if (msg.hasAttrUpdate()) {
            TbAttributeUpdateProto proto = msg.getAttrUpdate();
            subscriptionManagerService.onAttributesUpdate(
                    toTenantId(proto.getTenantIdMSB(), proto.getTenantIdLSB()),
                    TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                    proto.getScope(), KvProtoUtil.toAttributeKvList(proto.getDataList()), callback);
        } else if (msg.hasAttrDelete()) {
            TbAttributeDeleteProto proto = msg.getAttrDelete();
            if (proto.hasNotifyDevice()) {
                // handles old messages with deprecated 'notifyDevice'
                subscriptionManagerService.onAttributesDelete(
                        toTenantId(proto.getTenantIdMSB(), proto.getTenantIdLSB()),
                        TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                        proto.getScope(), proto.getKeysList(), proto.getNotifyDevice(), callback);
            } else {
                // handles new messages without 'notifyDevice'
                subscriptionManagerService.onAttributesDelete(
                        toTenantId(proto.getTenantIdMSB(), proto.getTenantIdLSB()),
                        TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                        proto.getScope(), proto.getKeysList(), callback);
            }
        } else if (msg.hasTsDelete()) {
            TbTimeSeriesDeleteProto proto = msg.getTsDelete();
            subscriptionManagerService.onTimeSeriesDelete(
                    toTenantId(proto.getTenantIdMSB(), proto.getTenantIdLSB()),
                    TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                    proto.getKeysList(), callback);
        } else if (msg.hasAlarmUpdate()) {
            TbAlarmUpdateProto proto = msg.getAlarmUpdate();
            subscriptionManagerService.onAlarmUpdate(
                    toTenantId(proto.getTenantIdMSB(), proto.getTenantIdLSB()),
                    TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                    JacksonUtil.fromString(proto.getAlarm(), AlarmInfo.class),
                    callback);
        } else if (msg.hasAlarmDelete()) {
            TbAlarmDeleteProto proto = msg.getAlarmDelete();
            subscriptionManagerService.onAlarmDeleted(
                    toTenantId(proto.getTenantIdMSB(), proto.getTenantIdLSB()),
                    TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                    JacksonUtil.fromString(proto.getAlarm(), AlarmInfo.class), callback);
        } else if (msg.hasNotificationUpdate()) {
            TransportProtos.NotificationUpdateProto updateProto = msg.getNotificationUpdate();
            TenantId tenantId = toTenantId(updateProto.getTenantIdMSB(), updateProto.getTenantIdLSB());
            UserId recipientId = new UserId(new UUID(updateProto.getRecipientIdMSB(), updateProto.getRecipientIdLSB()));
            NotificationUpdate update = JacksonUtil.fromString(updateProto.getUpdate(), NotificationUpdate.class);
            subscriptionManagerService.onNotificationUpdate(tenantId, recipientId, update, callback);
        } else if (msg.hasNotificationRequestUpdate()) {
            TransportProtos.NotificationRequestUpdateProto updateProto = msg.getNotificationRequestUpdate();
            TenantId tenantId = toTenantId(updateProto.getTenantIdMSB(), updateProto.getTenantIdLSB());
            NotificationRequestUpdate update = JacksonUtil.fromString(updateProto.getUpdate(), NotificationRequestUpdate.class);
            localSubscriptionService.onNotificationRequestUpdate(tenantId, update, callback);
        } else {
            throwNotHandled(msg, callback);
        }
        if (statsEnabled) {
            stats.log(msg);
        }
    }


    /**
     * 将通用设备状态队列消息同步交给 {@link DeviceStateService}（由其负责 callback）。
     */
    void forwardToStateService(DeviceStateServiceMsgProto deviceStateServiceMsg, TbCallback callback) {
        if (statsEnabled) {
            stats.log(deviceStateServiceMsg);
        }
        stateService.onQueueMsg(deviceStateServiceMsg, callback);
    }

    /**
     * 异步处理设备连接事件：提交到 {@link #deviceActivityEventsExecutor}，完成后映射到 pack callback。
     */
    void forwardToStateService(TransportProtos.DeviceConnectProto deviceConnectMsg, TbCallback callback) {
        if (statsEnabled) {
            stats.log(deviceConnectMsg);
        }
        var tenantId = toTenantId(deviceConnectMsg.getTenantIdMSB(), deviceConnectMsg.getTenantIdLSB());
        var deviceId = new DeviceId(new UUID(deviceConnectMsg.getDeviceIdMSB(), deviceConnectMsg.getDeviceIdLSB()));
        ListenableFuture<?> future = deviceActivityEventsExecutor.submit(() -> stateService.onDeviceConnect(tenantId, deviceId, deviceConnectMsg.getLastConnectTime()));
        DonAsynchron.withCallback(future,
                __ -> callback.onSuccess(),
                t -> {
                    log.warn("[{}] Failed to process device connect message for device [{}]", tenantId.getId(), deviceId.getId(), t);
                    callback.onFailure(t);
                });
    }

    /**
     * 异步处理设备活跃心跳/活动时间更新。
     */
    void forwardToStateService(TransportProtos.DeviceActivityProto deviceActivityMsg, TbCallback callback) {
        if (statsEnabled) {
            stats.log(deviceActivityMsg);
        }
        var tenantId = toTenantId(deviceActivityMsg.getTenantIdMSB(), deviceActivityMsg.getTenantIdLSB());
        var deviceId = new DeviceId(new UUID(deviceActivityMsg.getDeviceIdMSB(), deviceActivityMsg.getDeviceIdLSB()));
        ListenableFuture<?> future = deviceActivityEventsExecutor.submit(() -> stateService.onDeviceActivity(tenantId, deviceId, deviceActivityMsg.getLastActivityTime()));
        DonAsynchron.withCallback(future,
                __ -> callback.onSuccess(),
                t -> {
                    log.warn("[{}] Failed to process device activity message for device [{}]", tenantId.getId(), deviceId.getId(), t);
                    callback.onFailure(new RuntimeException("Failed to update device activity for device [" + deviceId.getId() + "]!", t));
                });
    }

    /**
     * 异步处理设备断开连接事件。
     */
    void forwardToStateService(TransportProtos.DeviceDisconnectProto deviceDisconnectMsg, TbCallback callback) {
        if (statsEnabled) {
            stats.log(deviceDisconnectMsg);
        }
        var tenantId = toTenantId(deviceDisconnectMsg.getTenantIdMSB(), deviceDisconnectMsg.getTenantIdLSB());
        var deviceId = new DeviceId(new UUID(deviceDisconnectMsg.getDeviceIdMSB(), deviceDisconnectMsg.getDeviceIdLSB()));
        ListenableFuture<?> future = deviceActivityEventsExecutor.submit(() -> stateService.onDeviceDisconnect(tenantId, deviceId, deviceDisconnectMsg.getLastDisconnectTime()));
        DonAsynchron.withCallback(future,
                __ -> callback.onSuccess(),
                t -> {
                    log.warn("[{}] Failed to process device disconnect message for device [{}]", tenantId.getId(), deviceId.getId(), t);
                    callback.onFailure(t);
                });
    }

    /**
     * 异步处理设备进入不活跃状态的事件。
     */
    void forwardToStateService(TransportProtos.DeviceInactivityProto deviceInactivityMsg, TbCallback callback) {
        if (statsEnabled) {
            stats.log(deviceInactivityMsg);
        }
        var tenantId = toTenantId(deviceInactivityMsg.getTenantIdMSB(), deviceInactivityMsg.getTenantIdLSB());
        var deviceId = new DeviceId(new UUID(deviceInactivityMsg.getDeviceIdMSB(), deviceInactivityMsg.getDeviceIdLSB()));
        ListenableFuture<?> future = deviceActivityEventsExecutor.submit(() -> stateService.onDeviceInactivity(tenantId, deviceId, deviceInactivityMsg.getLastInactivityTime()));
        DonAsynchron.withCallback(future,
                __ -> callback.onSuccess(),
                t -> {
                    log.warn("[{}] Failed to process device inactivity message for device [{}]", tenantId.getId(), deviceId.getId(), t);
                    callback.onFailure(t);
                });
    }

    /**
     * 异步处理设备不活跃超时阈值变更（例如设备配置调整后的超时时间更新）。
     */
    void forwardToStateService(TransportProtos.DeviceInactivityTimeoutUpdateProto deviceInactivityTimeoutUpdateMsg, TbCallback callback) {
        if (statsEnabled) {
            stats.log(deviceInactivityTimeoutUpdateMsg);
        }
        var tenantId = toTenantId(deviceInactivityTimeoutUpdateMsg.getTenantIdMSB(), deviceInactivityTimeoutUpdateMsg.getTenantIdLSB());
        var deviceId = new DeviceId(new UUID(deviceInactivityTimeoutUpdateMsg.getDeviceIdMSB(), deviceInactivityTimeoutUpdateMsg.getDeviceIdLSB()));
        ListenableFuture<?> future = deviceActivityEventsExecutor.submit(() -> stateService.onDeviceInactivityTimeoutUpdate(tenantId, deviceId, deviceInactivityTimeoutUpdateMsg.getInactivityTimeout()));
        DonAsynchron.withCallback(future,
                __ -> callback.onSuccess(),
                t -> {
                    log.warn("[{}] Failed to process device inactivity timeout update message for device [{}]", tenantId.getId(), deviceId.getId(), t);
                    callback.onFailure(t);
                });
    }

    /**
     * 将通知调度请求交给 {@link NotificationSchedulerService}；调度失败时以失败回调结束本条消息。
     */
    private void forwardToNotificationSchedulerService(TransportProtos.NotificationSchedulerServiceMsg msg, TbCallback callback) {
        TenantId tenantId = toTenantId(msg.getTenantIdMSB(), msg.getTenantIdLSB());
        NotificationRequestId notificationRequestId = new NotificationRequestId(new UUID(msg.getRequestIdMSB(), msg.getRequestIdLSB()));
        try {
            notificationSchedulerService.scheduleNotificationRequest(tenantId, notificationRequestId, msg.getTs());
            callback.onSuccess();
        } catch (Exception e) {
            callback.onFailure(new RuntimeException("Failed to schedule notification request", e));
        }
    }

    /**
     * 将传输层到设备 Actor 的消息包装后投入 Actor 系统；callback 随 wrapper 传递，由 Actor 处理路径触发。
     */
    private void forwardToDeviceActor(TransportToDeviceActorMsg toDeviceActorMsg, TbCallback callback) {
        if (statsEnabled) {
            stats.log(toDeviceActorMsg);
        }
        actorContext.tell(new TransportToDeviceActorMsgWrapper(toDeviceActorMsg, callback));
    }

    /**
     * 将错误事件协议转为 {@link ErrorEvent} 并异步落库。
     */
    private void forwardToEventService(ErrorEventProto eventProto, TbCallback callback) {
        Event event = ErrorEvent.builder()
                .tenantId(toTenantId(eventProto.getTenantIdMSB(), eventProto.getTenantIdLSB()))
                .entityId(new UUID(eventProto.getEntityIdMSB(), eventProto.getEntityIdLSB()))
                .serviceId(eventProto.getServiceId())
                .ts(System.currentTimeMillis())
                .method(eventProto.getMethod())
                .error(eventProto.getError())
                .build();
        forwardToEventService(event, callback);
    }

    /**
     * 将生命周期事件协议转为 {@link LifecycleEvent} 并异步落库。
     */
    private void forwardToEventService(LifecycleEventProto eventProto, TbCallback callback) {
        Event event = LifecycleEvent.builder()
                .tenantId(toTenantId(eventProto.getTenantIdMSB(), eventProto.getTenantIdLSB()))
                .entityId(new UUID(eventProto.getEntityIdMSB(), eventProto.getEntityIdLSB()))
                .serviceId(eventProto.getServiceId())
                .ts(System.currentTimeMillis())
                .lcEventType(eventProto.getLcEventType())
                .success(eventProto.getSuccess())
                .error(StringUtils.isNotEmpty(eventProto.getError()) ? eventProto.getError() : null)
                .build();
        forwardToEventService(event, callback);
    }

    /**
     * 通过 {@link ActorSystemContext#getEventService()} 异步保存事件，
     * 成功/失败映射到队列消息 callback；DB 回调在 {@code dbCallbackExecutor} 上执行。
     */
    private void forwardToEventService(Event event, TbCallback callback) {
        DonAsynchron.withCallback(actorContext.getEventService().saveAsync(event),
                result -> callback.onSuccess(),
                callback::onFailure,
                actorContext.getDbCallbackExecutor());
    }

    /**
     * 将 REST API 经规则引擎执行后的响应交给 {@link RuleEngineCallService}，解除调用方等待。
     */
    void forwardToRuleEngineCallService(TransportProtos.RestApiCallResponseMsgProto restApiCallResponseMsg, TbCallback callback) {
        ruleEngineCallService.onQueueMsg(restApiCallResponseMsg, callback);
    }

    /**
     * 无法识别的消息类型：记警告日志并以失败回调结束，避免静默丢弃导致难以排查。
     */
    private void throwNotHandled(Object msg, TbCallback callback) {
        log.warn("Message not handled: {}", msg);
        callback.onFailure(new RuntimeException("Message not handled!"));
    }

    /**
     * 由 MSB/LSB 构造 {@link TenantId} 的便捷方法。
     */
    private TenantId toTenantId(long tenantIdMSB, long tenantIdLSB) {
        return TenantId.fromUUID(new UUID(tenantIdMSB, tenantIdLSB));
    }

    /**
     * 停止本服务拥有的全部消费者。
     * <p>
     * 先停父类通知消费者，再停主通道（发停止信号并 await），最后停用量统计与 OTA 消费者。
     */
    @Override
    protected void stopConsumers() {
        super.stopConsumers();
        mainConsumer.stop();
        mainConsumer.awaitStop();
        usageStatsConsumer.stop();
        firmwareStatesConsumer.stop();
    }

}
