/**
 * Copyright 漏 2016-2025 The Thingsboard Authors
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

@Service
@Slf4j
@TbCoreComponent
public class DefaultTbCoreConsumerService extends AbstractConsumerService<ToCoreNotificationMsg> implements TbCoreConsumerService {

    @Value("${queue.core.poll-interval}")
    private long pollInterval;
    @Value("${queue.core.pack-processing-timeout}")
    private long packProcessingTimeout;
    @Value("${queue.core.consumer-per-partition:true}")
    private boolean consumerPerPartition;
    @Value("${queue.core.stats.enabled:false}")
    private boolean statsEnabled;

    @Value("${queue.core.ota.pack-interval-ms:60000}")
    private long firmwarePackInterval;
    @Value("${queue.core.ota.pack-size:100}")
    private int firmwarePackSize;

    private final DeviceStateService stateService;
    private final TbApiUsageStateService statsService;
    private final TbLocalSubscriptionService localSubscriptionService;
    private final SubscriptionManagerService subscriptionManagerService;
    private final TbCoreDeviceRpcService tbCoreDeviceRpcService;
    private final OtaPackageStateService firmwareStateService;
    private final GitVersionControlQueueService vcQueueService;
    private final NotificationSchedulerService notificationSchedulerService;
    private final NotificationRuleProcessor notificationRuleProcessor;
    private final TbCoreQueueFactory queueFactory;
    private final TbImageService imageService;
    private final RuleEngineCallService ruleEngineCallService;
    private final EdqsService edqsService;
    private final TbCoreConsumerStats stats;

    // 鏍稿績娑堟伅娑堣垂鑰�
    private MainQueueConsumerManager<TbProtoQueueMsg<ToCoreMsg>, QueueConfig> mainConsumer;

    // 缁熻淇℃伅娑堣垂鑰�
    private QueueConsumerManager<TbProtoQueueMsg<ToUsageStatsServiceMsg>> usageStatsConsumer;

    // 鍥轰欢鐘舵€佹秷璐硅€�
    private QueueConsumerManager<TbProtoQueueMsg<ToOtaPackageStateServiceMsg>> firmwareStatesConsumer;

    private volatile ListeningExecutorService deviceActivityEventsExecutor;

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

    @PostConstruct
    public void init() {
        super.init("tb-core");
        // 璁惧娲昏穬浜嬩欢鎵ц鍣�
        this.deviceActivityEventsExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(ThingsBoardThreadFactory.forName("tb-core-device-activity-events-executor")));

        // 鏋勫缓鏍稿績娑堟伅娑堣垂鑰咃紙澶勭悊璁惧鐘舵€�/RPC/璁㈤槄绛夋秷鎭級
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

    @PreDestroy
    public void destroy() {
        super.destroy();
        if (deviceActivityEventsExecutor != null) {
            deviceActivityEventsExecutor.shutdownNow();
        }
    }

    @Override
    protected void startConsumers() {
        // 鍚姩閫氱煡娑堟伅娑堣垂鑰�
        super.startConsumers();
        // 鍚姩鍥轰欢鐘舵€佹秷璐硅€�
        firmwareStatesConsumer.subscribe();
        firmwareStatesConsumer.launch();
        // 鍚姩缁熻淇℃伅娑堣垂鑰�
        usageStatsConsumer.launch();
    }

    /**
     * 褰撳垎鍖哄彂鐢熸敼鍙樻椂瑙﹀彂
     * @param event
     */
    @Override
    protected void onTbApplicationEvent(PartitionChangeEvent event) {
        log.debug("Subscribing to partitions: {}", event.getCorePartitions());
        // 杩欓噷鑾峰彇璇ユ湇鍔″櫒瀹炰緥鐨勬牳蹇冭闃呭垎鍖哄苟璁㈤槄topic
        mainConsumer.update(event.getCorePartitions());
        // 鏇存柊缁熻娑堣垂鑰呭垎鍖�
        usageStatsConsumer.subscribe(event.getCorePartitions()
                .stream()
                .map(tpi -> tpi.newByTopic(usageStatsConsumer.getConsumer().getTopic()))
                .collect(Collectors.toSet()));
    }

    /**
     * 娑堟伅澶勭悊瀹為檯閫昏緫
     * @param msgs
     * @param consumer
     * @param config
     * @throws Exception
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
                    // 娑堟伅璺敱 - 鏍规嵁娑堟伅绫诲瀷鍒嗗彂鍒颁笉鍚屾湇鍔�
                    if (toCoreMsg.hasToSubscriptionMgrMsg()) {
                        // 璁㈤槄绠＄悊鏈嶅姟娑堟伅
                        log.trace("[{}] Forwarding message to subscription manager service {}", id, toCoreMsg.getToSubscriptionMgrMsg());
                        forwardToSubMgrService(toCoreMsg.getToSubscriptionMgrMsg(), callback);
                    } else if (toCoreMsg.hasToDeviceActorMsg()) {
                        // 璁惧actor娑堟伅
                        log.trace("[{}] Forwarding message to device actor {}", id, toCoreMsg.getToDeviceActorMsg());
                        forwardToDeviceActor(toCoreMsg.getToDeviceActorMsg(), callback);
                    } else if (toCoreMsg.hasDeviceStateServiceMsg()) {
                        // 璁惧鐘舵€佹湇鍔℃秷鎭�
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceStateServiceMsg());
                        forwardToStateService(toCoreMsg.getDeviceStateServiceMsg(), callback);
                    } else if (toCoreMsg.hasDeviceConnectMsg()) {
                        // 璁惧杩炴帴浜嬩欢
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceConnectMsg());
                        forwardToStateService(toCoreMsg.getDeviceConnectMsg(), callback);
                    } else if (toCoreMsg.hasDeviceActivityMsg()) {
                        // 璁惧娲诲姩浜嬩欢
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceActivityMsg());
                        forwardToStateService(toCoreMsg.getDeviceActivityMsg(), callback);
                    } else if (toCoreMsg.hasDeviceDisconnectMsg()) {
                        // 璁惧鏂紑杩炴帴浜嬩欢
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceDisconnectMsg());
                        forwardToStateService(toCoreMsg.getDeviceDisconnectMsg(), callback);
                    } else if (toCoreMsg.hasDeviceInactivityMsg()) {
                        // 璁惧涓嶆椿鍔ㄤ簨浠�
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceInactivityMsg());
                        forwardToStateService(toCoreMsg.getDeviceInactivityMsg(), callback);
                    } else if (toCoreMsg.hasDeviceInactivityTimeoutUpdateMsg()) {
                        // 璁惧涓嶆椿鍔ㄨ秴鏃舵洿鏂�
                        log.trace("[{}] Forwarding message to device state service {}", id, toCoreMsg.getDeviceInactivityTimeoutUpdateMsg());
                        forwardToStateService(toCoreMsg.getDeviceInactivityTimeoutUpdateMsg(), callback);
                    } else if (toCoreMsg.hasToDeviceActorNotification()) {
                        // 璁惧Actor閫氱煡锛堝RPC璇锋眰锛�
                        TbActorMsg actorMsg = ProtoUtils.fromProto(toCoreMsg.getToDeviceActorNotification());
                        if (actorMsg != null) {
                            if (actorMsg.getMsgType().equals(MsgType.DEVICE_RPC_REQUEST_TO_DEVICE_ACTOR_MSG)) {
                                // 澶勭悊璁惧RPC璇锋眰
                                tbCoreDeviceRpcService.forwardRpcRequestToDeviceActor((ToDeviceRpcRequestActorMsg) actorMsg);
                            } else {
                                // 澶勭悊鍏跺畠
                                log.trace("[{}] Forwarding message to App Actor {}", id, actorMsg);
                                actorContext.tell(actorMsg);
                            }
                        }
                        callback.onSuccess();
                    } else if (toCoreMsg.hasNotificationSchedulerServiceMsg()) {
                        // 閫氱煡璋冨害鏈嶅姟娑堟伅
                        TransportProtos.NotificationSchedulerServiceMsg notificationSchedulerServiceMsg = toCoreMsg.getNotificationSchedulerServiceMsg();
                        log.trace("[{}] Forwarding message to notification scheduler service {}", id, toCoreMsg.getNotificationSchedulerServiceMsg());
                        forwardToNotificationSchedulerService(notificationSchedulerServiceMsg, callback);
                    } else if (toCoreMsg.hasErrorEventMsg()) {
                        // 閿欒浜嬩欢
                        forwardToEventService(toCoreMsg.getErrorEventMsg(), callback);
                    } else if (toCoreMsg.hasLifecycleEventMsg()) {
                        // 鐢熷懡鍛ㄦ湡浜嬩欢
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

    @Override
    protected ServiceType getServiceType() {
        return ServiceType.TB_CORE;
    }

    @Override
    protected long getNotificationPollDuration() {
        return pollInterval;
    }

    @Override
    protected long getNotificationPackProcessingTimeout() {
        return packProcessingTimeout;
    }

    @Override
    protected int getMgmtThreadPoolSize() {
        return Math.max(Runtime.getRuntime().availableProcessors(), 4);
    }

    @Override
    protected TbQueueConsumer<TbProtoQueueMsg<ToCoreNotificationMsg>> createNotificationsConsumer() {
        return queueFactory.createToCoreNotificationsMsgConsumer();
    }

    /**
     * 澶勭悊閫氱煡娑堟伅 - 鏉ヨ嚜鍏朵粬鏈嶅姟鐨勯€氱煡
     * @param id 娑堟伅ID
     * @param msg 鍗忚娑堟伅
     * @param callback 鍥炶皟鎺ュ彛
     */
    @Override
    protected void handleNotification(UUID id, TbProtoQueueMsg<ToCoreNotificationMsg> msg, TbCallback callback) {
        ToCoreNotificationMsg toCoreNotification = msg.getValue();
        // 鏍规嵁娑堟伅绫诲瀷璺敱鍒颁笉鍚屽鐞嗗櫒
        if (toCoreNotification.hasToLocalSubscriptionServiceMsg()) {
            // 鏈湴璁㈤槄鏈嶅姟娑堟伅
            log.trace("[{}] Forwarding message to local subscription service {}", id, toCoreNotification.getToLocalSubscriptionServiceMsg());
            forwardToLocalSubMgrService(toCoreNotification.getToLocalSubscriptionServiceMsg(), callback);
        } else if (toCoreNotification.hasCoreStartupMsg()) {
            // core鍚姩娑堟伅锛堢敤浜巆ore鍚姩澶氫釜鏃讹紝閲嶆柊鍒嗛厤璁㈤槄锛�
            log.trace("[{}] Forwarding message to local subscription service {}", id, toCoreNotification.getCoreStartupMsg());
            forwardCoreStartupMsg(toCoreNotification.getCoreStartupMsg(), callback);
        } else if (toCoreNotification.hasFromDeviceRpcResponse()) {
            // 璁惧RPC鍝嶅簲
            log.trace("[{}] Forwarding message to RPC service {}", id, toCoreNotification.getFromDeviceRpcResponse());
            forwardToCoreRpcService(toCoreNotification.getFromDeviceRpcResponse(), callback);
        } else if (toCoreNotification.hasRestApiCallResponseMsg()) {
            // REST API璋冪敤鍝嶅簲
            log.trace("[{}] Forwarding message to RuleEngineCallService service {}", id, toCoreNotification.getRestApiCallResponseMsg());
            forwardToRuleEngineCallService(toCoreNotification.getRestApiCallResponseMsg(), callback);
        } else if (toCoreNotification.hasComponentLifecycle()) {
            // 缁勪欢鐢熷懡鍛ㄦ湡娑堟伅
            handleComponentLifecycleMsg(id, ProtoUtils.fromProto(toCoreNotification.getComponentLifecycle()));
            callback.onSuccess();
        } else if (toCoreNotification.getQueueUpdateMsgsCount() > 0) {
            // 闃熷垪鏇存柊娑堟伅
            partitionService.updateQueues(toCoreNotification.getQueueUpdateMsgsList());
            callback.onSuccess();
        } else if (toCoreNotification.getQueueDeleteMsgsCount() > 0) {
            // 闃熷垪鍒犻櫎娑堟伅
            partitionService.removeQueues(toCoreNotification.getQueueDeleteMsgsList());
            callback.onSuccess();
        } else if (toCoreNotification.hasVcResponseMsg()) {
            // 鐗堟湰鎺у埗鍝嶅簲
            vcQueueService.processResponse(toCoreNotification.getVcResponseMsg());
            callback.onSuccess();
        } else if (toCoreNotification.hasToSubscriptionMgrMsg()) {
            // 璁㈤槄绠＄悊鏈嶅姟娑堟伅
            forwardToSubMgrService(toCoreNotification.getToSubscriptionMgrMsg(), callback);
        } else if (toCoreNotification.hasNotificationRuleProcessorMsg()) {
            // 閫氱煡瑙勫垯澶勭悊
            NotificationRuleTrigger notificationRuleTrigger =
                    JavaSerDesUtil.decode(toCoreNotification.getNotificationRuleProcessorMsg().getTrigger().toByteArray());
            notificationRuleProcessor.process(notificationRuleTrigger);
            callback.onSuccess();
        } else if (toCoreNotification.hasResourceCacheInvalidateMsg()) {
            // 璧勬簮缂撳瓨澶辨晥
            forwardToResourceService(toCoreNotification.getResourceCacheInvalidateMsg(), callback);
        } else if (toCoreNotification.hasToEdqsCoreServiceMsg()) {
            // EDQS绯荤粺娑堟伅锛坋dqs鏁版嵁鍚屾锛�
            edqsService.processSystemMsg(JacksonUtil.fromBytes(toCoreNotification.getToEdqsCoreServiceMsg().getValue().toByteArray(), ToCoreEdqsMsg.class));
            callback.onSuccess();
        }
        // 缁熻璁板綍
        if (statsEnabled) {
            stats.log(toCoreNotification);
        }
    }

    /**
     * 澶勭悊浣跨敤缁熻娑堟伅
     * @param msgs 娑堟伅鎵规
     * @param consumer 娑堣垂鑰�
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
     * 澶勭悊鍥轰欢鐘舵€佹秷鎭�
     * @param msgs 娑堟伅鎵规
     * @param consumer 娑堣垂鑰�
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

    private void handleUsageStats(TbProtoQueueMsg<ToUsageStatsServiceMsg> msg, TbCallback callback) {
        statsService.process(msg, callback);
    }

    private boolean handleOtaPackageUpdates(TbProtoQueueMsg<ToOtaPackageStateServiceMsg> msg) {
        return firmwareStateService.process(msg.getValue());
    }

    private void forwardToCoreRpcService(FromDeviceRPCResponseProto proto, TbCallback callback) {
        RpcError error = proto.getError() > 0 ? RpcError.values()[proto.getError()] : null;
        FromDeviceRpcResponse response = new FromDeviceRpcResponse(new UUID(proto.getRequestIdMSB(), proto.getRequestIdLSB())
                , proto.getResponse(), error);
        tbCoreDeviceRpcService.processRpcResponseFromRuleEngine(response);
        callback.onSuccess();
    }

    @Scheduled(fixedDelayString = "${queue.core.stats.print-interval-ms}")
    public void printStats() {
        if (statsEnabled) {
            stats.printStats();
            stats.reset();
        }
    }

    /**
     * 杞彂鏉ヨ嚜璁㈤槄绠＄悊鏈嶅姟鐨勬秷鎭埌鏈湴璁㈤槄鏈嶅姟
     * @param msg 鏈湴璁㈤槄鏈嶅姟鍗忚娑堟伅
     * @param callback 鍥炶皟鎺ュ彛
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
     * 杞彂鏍稿績鍚姩娑堟伅
     * @param coreStartupMsg 鏍稿績鍚姩鍗忚娑堟伅
     * @param callback 鍥炶皟鎺ュ彛
     */
    private void forwardCoreStartupMsg(TransportProtos.CoreStartupMsg coreStartupMsg, TbCallback callback) {
        log.info("[{}] Processing core startup with partitions: {}", coreStartupMsg.getServiceId(), coreStartupMsg.getPartitionsList());
        localSubscriptionService.onCoreStartupMsg(coreStartupMsg);
        callback.onSuccess();
    }

    /**
     * 杞彂鍒拌祫婧愭湇鍔★紙澶勭悊鍥剧墖缂撳瓨澶辨晥锛�
     * @param msg 璧勬簮缂撳瓨澶辨晥鍗忚娑堟伅
     * @param callback 鍥炶皟鎺ュ彛
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
     * 杞彂鍒拌闃呯鐞嗘湇鍔�
     * @param msg 璁㈤槄绠＄悊鍗忚娑堟伅
     * @param callback 鍥炶皟鎺ュ彛
     */
    private void forwardToSubMgrService(SubscriptionMgrMsgProto msg, TbCallback callback) {
        if (msg.hasSubEvent()) {
            // 璁㈤槄浜嬩欢澶勭悊锛屽幓璁㈤槄璇ュ疄浣�
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
            // 鏃跺簭鏁版嵁鏇存柊
            TbTimeSeriesUpdateProto proto = msg.getTsUpdate();
            long tenantIdMSB = proto.getTenantIdMSB();
            long tenantIdLSB = proto.getTenantIdLSB();
            subscriptionManagerService.onTimeSeriesUpdate(
                    toTenantId(tenantIdMSB, tenantIdLSB),
                    TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                    KvProtoUtil.fromTsKvProtoList(proto.getDataList()), callback);
        } else if (msg.hasAttrUpdate()) {
            // 灞炴€ф洿鏂�
            TbAttributeUpdateProto proto = msg.getAttrUpdate();
            subscriptionManagerService.onAttributesUpdate(
                    toTenantId(proto.getTenantIdMSB(), proto.getTenantIdLSB()),
                    TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                    proto.getScope(), KvProtoUtil.toAttributeKvList(proto.getDataList()), callback);
        } else if (msg.hasAttrDelete()) {
            // 灞炴€ф洿鏂�
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
            // 鏃跺簭鏁版嵁鍒犻櫎
            TbTimeSeriesDeleteProto proto = msg.getTsDelete();
            subscriptionManagerService.onTimeSeriesDelete(
                    toTenantId(proto.getTenantIdMSB(), proto.getTenantIdLSB()),
                    TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                    proto.getKeysList(), callback);
        } else if (msg.hasAlarmUpdate()) {
            // 鍛婅鏇存柊
            TbAlarmUpdateProto proto = msg.getAlarmUpdate();
            subscriptionManagerService.onAlarmUpdate(
                    toTenantId(proto.getTenantIdMSB(), proto.getTenantIdLSB()),
                    TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                    JacksonUtil.fromString(proto.getAlarm(), AlarmInfo.class),
                    callback);
        } else if (msg.hasAlarmDelete()) {
            // 鍛婅鍒犻櫎
            TbAlarmDeleteProto proto = msg.getAlarmDelete();
            subscriptionManagerService.onAlarmDeleted(
                    toTenantId(proto.getTenantIdMSB(), proto.getTenantIdLSB()),
                    TbSubscriptionUtils.toEntityId(proto.getEntityType(), proto.getEntityIdMSB(), proto.getEntityIdLSB()),
                    JacksonUtil.fromString(proto.getAlarm(), AlarmInfo.class), callback);
        } else if (msg.hasNotificationUpdate()) {
            // 閫氱煡鏇存柊
            TransportProtos.NotificationUpdateProto updateProto = msg.getNotificationUpdate();
            TenantId tenantId = toTenantId(updateProto.getTenantIdMSB(), updateProto.getTenantIdLSB());
            UserId recipientId = new UserId(new UUID(updateProto.getRecipientIdMSB(), updateProto.getRecipientIdLSB()));
            NotificationUpdate update = JacksonUtil.fromString(updateProto.getUpdate(), NotificationUpdate.class);
            subscriptionManagerService.onNotificationUpdate(tenantId, recipientId, update, callback);
        } else if (msg.hasNotificationRequestUpdate()) {
            // 閫氱煡璇锋眰鏇存柊
            TransportProtos.NotificationRequestUpdateProto updateProto = msg.getNotificationRequestUpdate();
            TenantId tenantId = toTenantId(updateProto.getTenantIdMSB(), updateProto.getTenantIdLSB());
            NotificationRequestUpdate update = JacksonUtil.fromString(updateProto.getUpdate(), NotificationRequestUpdate.class);
            localSubscriptionService.onNotificationRequestUpdate(tenantId, update, callback);
        } else {
            // 鏈鐞嗙殑娑堟伅绫诲瀷
            throwNotHandled(msg, callback);
        }
        // 缁熻璁板綍
        if (statsEnabled) {
            stats.log(msg);
        }
    }


    void forwardToStateService(DeviceStateServiceMsgProto deviceStateServiceMsg, TbCallback callback) {
        if (statsEnabled) {
            stats.log(deviceStateServiceMsg);
        }
        stateService.onQueueMsg(deviceStateServiceMsg, callback);
    }

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

    private void forwardToDeviceActor(TransportToDeviceActorMsg toDeviceActorMsg, TbCallback callback) {
        if (statsEnabled) {
            stats.log(toDeviceActorMsg);
        }
        actorContext.tell(new TransportToDeviceActorMsgWrapper(toDeviceActorMsg, callback));
    }

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

    private void forwardToEventService(Event event, TbCallback callback) {
        DonAsynchron.withCallback(actorContext.getEventService().saveAsync(event),
                result -> callback.onSuccess(),
                callback::onFailure,
                actorContext.getDbCallbackExecutor());
    }

    void forwardToRuleEngineCallService(TransportProtos.RestApiCallResponseMsgProto restApiCallResponseMsg, TbCallback callback) {
        ruleEngineCallService.onQueueMsg(restApiCallResponseMsg, callback);
    }

    private void throwNotHandled(Object msg, TbCallback callback) {
        log.warn("Message not handled: {}", msg);
        callback.onFailure(new RuntimeException("Message not handled!"));
    }

    private TenantId toTenantId(long tenantIdMSB, long tenantIdLSB) {
        return TenantId.fromUUID(new UUID(tenantIdMSB, tenantIdLSB));
    }

    @Override
    protected void stopConsumers() {
        super.stopConsumers();
        mainConsumer.stop();
        mainConsumer.awaitStop();
        usageStatsConsumer.stop();
        firmwareStatesConsumer.stop();
    }

}
