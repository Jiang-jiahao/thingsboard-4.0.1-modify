package com.jnks.iot.server.service.queue;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.QueueId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.common.data.rpc.RpcError;
import com.jnks.iot.server.common.msg.plugin.ComponentLifecycleMsg;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponse;
import com.jnks.iot.server.common.util.ProtoUtils;
import com.jnks.iot.server.dao.queue.QueueService;
import com.jnks.iot.server.dao.tenant.JnksIotTenantProfileCache;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.QueueDeleteMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.QueueUpdateMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineNotificationMsg;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.QueueKey;
import com.jnks.iot.server.queue.discovery.event.PartitionChangeEvent;
import com.jnks.iot.server.service.apiusage.JnksIotApiUsageStateService;
import com.jnks.iot.server.service.cf.CalculatedFieldCache;
import com.jnks.iot.server.service.profile.JnksIotAssetProfileCache;
import com.jnks.iot.server.service.profile.JnksIotDeviceProfileCache;
import com.jnks.iot.server.service.queue.processing.AbstractPartitionBasedConsumerService;
import com.jnks.iot.server.service.queue.ruleengine.JnksIotRuleEngineConsumerContext;
import com.jnks.iot.server.service.queue.ruleengine.JnksIotRuleEngineQueueConsumerManager;
import com.jnks.iot.server.service.rpc.JnksIotRuleEngineDeviceRpcService;
import com.jnks.iot.server.service.security.auth.jwt.settings.JwtSettingsService;

import java.util.Optional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Service
public class DefaultJnksIotRuleEngineConsumerService extends AbstractPartitionBasedConsumerService<ToRuleEngineNotificationMsg> implements JnksIotRuleEngineConsumerService {

    private final JnksIotRuleEngineConsumerContext ctx;
    private final QueueService queueService;
    private final JnksIotRuleEngineDeviceRpcService jnksIotDeviceRpcService;

    /**
     * 鎵ц寮曟搸瀛樺湪澶氫釜闃熷垪鐨勬儏鍐碉紝鎵€浠ヨ繖閲屽畾涔塵ap杩涜瀛樺偍
     */
    private final ConcurrentMap<QueueKey, JnksIotRuleEngineQueueConsumerManager> consumers = new ConcurrentHashMap<>();

    public DefaultJnksIotRuleEngineConsumerService(JnksIotRuleEngineConsumerContext ctx,
                                              ActorSystemContext actorContext,
                                              JnksIotRuleEngineDeviceRpcService jnksIotDeviceRpcService,
                                              QueueService queueService,
                                              JnksIotDeviceProfileCache deviceProfileCache,
                                              JnksIotAssetProfileCache assetProfileCache,
                                              JnksIotTenantProfileCache tenantProfileCache,
                                              JnksIotApiUsageStateService apiUsageStateService,
                                              PartitionService partitionService,
                                              ApplicationEventPublisher eventPublisher,
                                              Optional<JwtSettingsService> jwtSettingsService,
                                              CalculatedFieldCache calculatedFieldCache) {
        super(actorContext, tenantProfileCache, deviceProfileCache, assetProfileCache, calculatedFieldCache, apiUsageStateService, partitionService, eventPublisher, jwtSettingsService);
        this.ctx = ctx;
        this.jnksIotDeviceRpcService = jnksIotDeviceRpcService;
        this.queueService = queueService;
    }

    @Override
    protected void onStartUp() {
        List<Queue> queues = queueService.findAllQueues();
        for (Queue configuration : queues) {
            if (partitionService.isManagedByCurrentService(configuration.getTenantId())) {
                QueueKey queueKey = new QueueKey(ServiceType.JNKS_IOT_RULE_ENGINE, configuration);
                createConsumer(queueKey, configuration);
            }
        }
    }

    @Override
    protected void onPartitionChangeEvent(PartitionChangeEvent event) {
        event.getNewPartitions().forEach((queueKey, partitions) -> {
            // 濡傛灉鏄绠楀瓧娈甸槦鍒楋紝鍒欎笉澶勭悊
            if (DataConstants.CF_QUEUE_NAME.equals(queueKey.getQueueName()) || DataConstants.CF_STATES_QUEUE_NAME.equals(queueKey.getQueueName())) {
                return;
            }
            // 鍒ゆ柇璇ョ鎴锋槸鍚︾敱褰撳墠鏈嶅姟绠＄悊璐熻矗
            if (partitionService.isManagedByCurrentService(queueKey.getTenantId())) {
                var consumer = getConsumer(queueKey).orElseGet(() -> {
                    // 濡傛灉consumerManager涓嶅瓨鍦紝鍒欒繖閲岃繘琛屽垱寤�
                    Queue config = queueService.findQueueByTenantIdAndName(queueKey.getTenantId(), queueKey.getQueueName());
                    if (config == null) {
                        if (!partitions.isEmpty()) {
                            log.error("[{}] Queue configuration is missing", queueKey, new RuntimeException("stacktrace"));
                        }
                        return null;
                    }
                    // 鍒涘缓consumerManager
                    return createConsumer(queueKey, config);
                });
                // 闃熷垪瀛樺湪锛屽垯鏇存柊鍒嗗尯淇℃伅
                if (consumer != null) {
                    consumer.update(partitions);
                }
            }
        });
        // 娓呴櫎涓嶅睘浜庡綋鍓嶆湇鍔¤礋璐ｇ殑闃熷垪
        consumers.keySet().stream()
                .collect(Collectors.groupingBy(QueueKey::getTenantId))
                .forEach((tenantId, queueKeys) -> {
                    if (!partitionService.isManagedByCurrentService(tenantId)) {
                        queueKeys.forEach(queueKey -> {
                            removeConsumer(queueKey).ifPresent(JnksIotRuleEngineQueueConsumerManager::stop);
                        });
                    }
                });
    }

    @Override
    protected void stopConsumers() {
        super.stopConsumers();
        consumers.values().forEach(JnksIotRuleEngineQueueConsumerManager::stop);
        consumers.values().forEach(JnksIotRuleEngineQueueConsumerManager::awaitStop);
    }

    @Override
    protected ServiceType getServiceType() {
        return ServiceType.JNKS_IOT_RULE_ENGINE;
    }

    @Override
    protected String getPrefix() {
        return "jnks-iot-rule-engine";
    }

    @Override
    protected long getNotificationPollDuration() {
        return ctx.getPollDuration();
    }

    @Override
    protected long getNotificationPackProcessingTimeout() {
        return ctx.getPackProcessingTimeout();
    }

    @Override
    protected int getMgmtThreadPoolSize() {
        return ctx.getMgmtThreadPoolSize();
    }

    @Override
    protected JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> createNotificationsConsumer() {
        return ctx.getQueueFactory().createToRuleEngineNotificationsMsgConsumer();
    }

    /**
     * 澶勭悊閫氱煡淇℃伅
     * @param id 閫氱煡淇℃伅uuid
     * @param msg 淇℃伅鍐呭
     * @param callback 鍥炶皟鍑芥暟
     */
    @Override
    protected void handleNotification(UUID id, JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg> msg, JnksIotCallback callback) {
        ToRuleEngineNotificationMsg nfMsg = msg.getValue();
        if (nfMsg.hasComponentLifecycle()) {
            // 澶勭悊缁勪欢鐢熷懡鍛ㄦ湡娑堟伅
            handleComponentLifecycleMsg(id, ProtoUtils.fromProto(nfMsg.getComponentLifecycle()));
            callback.onSuccess();
        } else if (nfMsg.hasFromDeviceRpcResponse()) {
            // 澶勭悊璁惧RPC鍝嶅簲娑堟伅
            TransportProtos.FromDeviceRPCResponseProto proto = nfMsg.getFromDeviceRpcResponse();
            RpcError error = proto.getError() > 0 ? RpcError.values()[proto.getError()] : null;
            FromDeviceRpcResponse response = new FromDeviceRpcResponse(new UUID(proto.getRequestIdMSB(), proto.getRequestIdLSB())
                    , proto.getResponse(), error);
            jnksIotDeviceRpcService.processRpcResponseFromDevice(response);
            callback.onSuccess();
        } else if (nfMsg.getQueueUpdateMsgsCount() > 0) {
            // 澶勭悊闃熷垪鏇存柊娑堟伅
            updateQueues(nfMsg.getQueueUpdateMsgsList());
            callback.onSuccess();
        } else if (nfMsg.getQueueDeleteMsgsCount() > 0) {
            // 澶勭悊闃熷垪鍒犻櫎娑堟伅
            deleteQueues(nfMsg.getQueueDeleteMsgsList());
            callback.onSuccess();
        } else {
            // 鏈煡娑堟伅绫诲瀷
            log.trace("Received notification with missing handler");
            callback.onSuccess();
        }
    }

    private void updateQueues(List<QueueUpdateMsg> queueUpdateMsgs) {
        for (QueueUpdateMsg queueUpdateMsg : queueUpdateMsgs) {
            log.info("Received queue update msg: [{}]", queueUpdateMsg);
            TenantId tenantId = TenantId.fromUUID(new UUID(queueUpdateMsg.getTenantIdMSB(), queueUpdateMsg.getTenantIdLSB()));
            if (partitionService.isManagedByCurrentService(tenantId)) {
                QueueId queueId = new QueueId(new UUID(queueUpdateMsg.getQueueIdMSB(), queueUpdateMsg.getQueueIdLSB()));
                String queueName = queueUpdateMsg.getQueueName();
                QueueKey queueKey = new QueueKey(ServiceType.JNKS_IOT_RULE_ENGINE, queueName, tenantId);
                Queue queue = queueService.findQueueById(tenantId, queueId);

                getConsumer(queueKey).ifPresentOrElse(consumer -> consumer.update(queue),
                        () -> createConsumer(queueKey, queue));
            }
        }

        partitionService.updateQueues(queueUpdateMsgs);
        partitionService.recalculatePartitions(ctx.getServiceInfoProvider().getServiceInfo(),
                new ArrayList<>(partitionService.getOtherServices(ServiceType.JNKS_IOT_RULE_ENGINE)));
    }

    private void deleteQueues(List<QueueDeleteMsg> queueDeleteMsgs) {
        for (QueueDeleteMsg queueDeleteMsg : queueDeleteMsgs) {
            log.info("Received queue delete msg: [{}]", queueDeleteMsg);
            TenantId tenantId = TenantId.fromUUID(new UUID(queueDeleteMsg.getTenantIdMSB(), queueDeleteMsg.getTenantIdLSB()));
            QueueKey queueKey = new QueueKey(ServiceType.JNKS_IOT_RULE_ENGINE, queueDeleteMsg.getQueueName(), tenantId);
            removeConsumer(queueKey).ifPresent(consumer -> consumer.delete(true));
        }

        partitionService.removeQueues(queueDeleteMsgs);
        partitionService.recalculatePartitions(ctx.getServiceInfoProvider().getServiceInfo(), new ArrayList<>(partitionService.getOtherServices(ServiceType.JNKS_IOT_RULE_ENGINE)));
    }

    @EventListener
    public void handleComponentLifecycleEvent(ComponentLifecycleMsg event) {
        if (event.getEntityId().getEntityType() == EntityType.TENANT) {
            if (event.getEvent() == ComponentLifecycleEvent.DELETED) {
                List<QueueKey> toRemove = consumers.keySet().stream()
                        .filter(queueKey -> queueKey.getTenantId().equals(event.getTenantId()))
                        .toList();
                toRemove.forEach(queueKey -> {
                    removeConsumer(queueKey).ifPresent(consumer -> consumer.delete(false));
                });
            }
        }
    }

    private Optional<JnksIotRuleEngineQueueConsumerManager> getConsumer(QueueKey queueKey) {
        return Optional.ofNullable(consumers.get(queueKey));
    }

    private JnksIotRuleEngineQueueConsumerManager createConsumer(QueueKey queueKey, Queue queue) {
        var consumer = JnksIotRuleEngineQueueConsumerManager.create()
                .ctx(ctx)
                .queueKey(queueKey)
                .consumerExecutor(consumersExecutor)
                .scheduler(scheduler)
                .taskExecutor(mgmtExecutor)
                .build();
        consumers.put(queueKey, consumer);
        consumer.init(queue);
        return consumer;
    }

    private Optional<JnksIotRuleEngineQueueConsumerManager> removeConsumer(QueueKey queueKey) {
        return Optional.ofNullable(consumers.remove(queueKey));
    }

    @Scheduled(fixedDelayString = "${queue.rule-engine.stats.print-interval-ms}")
    public void printStats() {
        if (ctx.isStatsEnabled()) {
            long ts = System.currentTimeMillis();
            consumers.values().forEach(manager -> manager.printStats(ts));
        }
    }

}
