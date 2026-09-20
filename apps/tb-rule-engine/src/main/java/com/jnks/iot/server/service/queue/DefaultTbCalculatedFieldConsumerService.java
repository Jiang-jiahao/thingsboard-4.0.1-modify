package com.jnks.iot.server.service.queue;

import jakarta.annotation.PreDestroy;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.calculatedField.CalculatedFieldLinkedTelemetryMsg;
import com.jnks.iot.server.actors.calculatedField.CalculatedFieldTelemetryMsg;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.data.queue.QueueConfig;
import com.jnks.iot.server.common.msg.cf.CalculatedFieldPartitionChangeMsg;
import com.jnks.iot.server.common.msg.plugin.ComponentLifecycleMsg;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TbCallback;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.dao.tenant.TbTenantProfileCache;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldLinkedTelemetryMsgProto;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldTelemetryMsgProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldNotificationMsg;
import com.jnks.iot.server.queue.TbQueueConsumer;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;
import com.jnks.iot.server.queue.common.consumer.PartitionedQueueConsumerManager;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.QueueKey;
import com.jnks.iot.server.queue.discovery.event.PartitionChangeEvent;
import com.jnks.iot.server.queue.provider.TbRuleEngineQueueFactory;
import com.jnks.iot.server.service.apiusage.TbApiUsageStateService;
import com.jnks.iot.server.service.cf.CalculatedFieldCache;
import com.jnks.iot.server.service.cf.CalculatedFieldStateService;
import com.jnks.iot.server.service.profile.TbAssetProfileCache;
import com.jnks.iot.server.service.profile.TbDeviceProfileCache;
import com.jnks.iot.server.service.queue.processing.AbstractPartitionBasedConsumerService;
import com.jnks.iot.server.service.queue.processing.IdMsgPair;
import com.jnks.iot.server.service.security.auth.jwt.settings.JwtSettingsService;

import java.util.Optional;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class DefaultTbCalculatedFieldConsumerService extends AbstractPartitionBasedConsumerService<ToCalculatedFieldNotificationMsg> implements TbCalculatedFieldConsumerService {

    @Value("${queue.calculated_fields.poll_interval:25}")
    private long pollInterval;
    @Value("${queue.calculated_fields.pack_processing_timeout:60000}")
    private long packProcessingTimeout;

    private final TbRuleEngineQueueFactory queueFactory;
    private final CalculatedFieldStateService stateService;

    public DefaultTbCalculatedFieldConsumerService(TbRuleEngineQueueFactory tbQueueFactory,
                                                   ActorSystemContext actorContext,
                                                   TbDeviceProfileCache deviceProfileCache,
                                                   TbAssetProfileCache assetProfileCache,
                                                   TbTenantProfileCache tenantProfileCache,
                                                   TbApiUsageStateService apiUsageStateService,
                                                   PartitionService partitionService,
                                                   ApplicationEventPublisher eventPublisher,
                                                   Optional<JwtSettingsService> jwtSettingsService,
                                                   CalculatedFieldCache calculatedFieldCache,
                                                   CalculatedFieldStateService stateService) {
        super(actorContext, tenantProfileCache, deviceProfileCache, assetProfileCache, calculatedFieldCache, apiUsageStateService, partitionService,
                eventPublisher, jwtSettingsService);
        this.queueFactory = tbQueueFactory;
        this.stateService = stateService;
    }

    @Override
    protected void onStartUp() {
        var queueKey = new QueueKey(ServiceType.TB_RULE_ENGINE, DataConstants.CF_QUEUE_NAME);
        var eventConsumer = PartitionedQueueConsumerManager.<TbProtoQueueMsg<ToCalculatedFieldMsg>>create()
                .queueKey(queueKey)
                .topic(partitionService.getTopic(queueKey))
                .pollInterval(pollInterval)
                .msgPackProcessor(this::processMsgs)
                .consumerCreator((queueConfig, tpi) -> queueFactory.createToCalculatedFieldMsgConsumer(tpi))
                .queueAdmin(queueFactory.getCalculatedFieldQueueAdmin())
                .consumerExecutor(consumersExecutor)
                .scheduler(scheduler)
                .taskExecutor(mgmtExecutor)
                .build();
        stateService.init(eventConsumer);
    }

    @PreDestroy
    public void destroy() {
        super.destroy();
    }

    @Override
    protected void startConsumers() {
        super.startConsumers();
    }

    @Override
    protected void onPartitionChangeEvent(PartitionChangeEvent event) {
        try {
            event.getNewPartitions().forEach((queueKey, partitions) -> {
                if (DataConstants.CF_QUEUE_NAME.equals(queueKey.getQueueName())) {
                    stateService.restore(queueKey, partitions);
                }
            });
            // eventConsumer's partitions will be updated by stateService

            // Cleanup old entities after corresponding consumers are stopped.
            // Any periodic tasks need to check that the entity is still managed by the current server before processing.
            actorContext.tell(new CalculatedFieldPartitionChangeMsg());
        } catch (Throwable t) {
            log.error("Failed to process partition change event: {}", event, t);
        }
    }

    private void processMsgs(List<TbProtoQueueMsg<ToCalculatedFieldMsg>> msgs, TbQueueConsumer<TbProtoQueueMsg<ToCalculatedFieldMsg>> consumer, QueueConfig config) throws Exception {
        List<IdMsgPair<ToCalculatedFieldMsg>> orderedMsgList = msgs.stream().map(msg -> new IdMsgPair<>(UUID.randomUUID(), msg)).toList();
        ConcurrentMap<UUID, TbProtoQueueMsg<ToCalculatedFieldMsg>> pendingMap = orderedMsgList.stream().collect(
                Collectors.toConcurrentMap(IdMsgPair::getUuid, IdMsgPair::getMsg));
        CountDownLatch processingTimeoutLatch = new CountDownLatch(1);
        TbPackProcessingContext<TbProtoQueueMsg<ToCalculatedFieldMsg>> ctx = new TbPackProcessingContext<>(
                processingTimeoutLatch, pendingMap, new ConcurrentHashMap<>());
        PendingMsgHolder<ToCalculatedFieldMsg> pendingMsgHolder = new PendingMsgHolder<>();
        Future<?> packSubmitFuture = consumersExecutor.submit(() -> {
            orderedMsgList.forEach((element) -> {
                UUID id = element.getUuid();
                TbProtoQueueMsg<ToCalculatedFieldMsg> msg = element.getMsg();
                log.trace("[{}] Creating main callback for message: {}", id, msg.getValue());
                TbCallback callback = new TbPackCallback<>(id, ctx);
                try {
                    ToCalculatedFieldMsg toCfMsg = msg.getValue();
                    pendingMsgHolder.setMsg(toCfMsg);
                    if (toCfMsg.hasTelemetryMsg()) {
                        log.trace("[{}] Forwarding regular telemetry message for processing {}", id, toCfMsg.getTelemetryMsg());
                        forwardToActorSystem(toCfMsg.getTelemetryMsg(), callback);
                    } else if (toCfMsg.hasLinkedTelemetryMsg()) {
                        forwardToActorSystem(toCfMsg.getLinkedTelemetryMsg(), callback);
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
            ctx.getAckMap().forEach((id, msg) -> log.warn("[{}] Timeout to process message: {}", id, msg.getValue()));
            ctx.getFailedMap().forEach((id, msg) -> log.warn("[{}] Failed to process message: {}", id, msg.getValue()));
        }
        consumer.commit();
    }

    @Override
    protected ServiceType getServiceType() {
        return ServiceType.TB_RULE_ENGINE;
    }

    @Override
    protected String getPrefix() {
        return "tb-cf";
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
    protected TbQueueConsumer<TbProtoQueueMsg<ToCalculatedFieldNotificationMsg>> createNotificationsConsumer() {
        return queueFactory.createToCalculatedFieldNotificationMsgConsumer();
    }

    @Override
    protected void handleNotification(UUID id, TbProtoQueueMsg<ToCalculatedFieldNotificationMsg> msg, TbCallback callback) {
        ToCalculatedFieldNotificationMsg toCfNotification = msg.getValue();
        if (toCfNotification.hasLinkedTelemetryMsg()) {
            forwardToActorSystem(toCfNotification.getLinkedTelemetryMsg(), callback);
        }
    }

    @EventListener
    public void handleComponentLifecycleEvent(ComponentLifecycleMsg event) {
        if (event.getEntityId().getEntityType() == EntityType.TENANT) {
            if (event.getEvent() == ComponentLifecycleEvent.DELETED) {
                Set<TopicPartitionInfo> partitions = stateService.getPartitions();
                if (CollectionUtils.isEmpty(partitions)) {
                    return;
                }
                stateService.delete(partitions.stream()
                        .filter(tpi -> tpi.getTenantId().isPresent() && tpi.getTenantId().get().equals(event.getTenantId()))
                        .collect(Collectors.toSet()));
            }
        }
    }

    private void forwardToActorSystem(CalculatedFieldTelemetryMsgProto msg, TbCallback callback) {
        var tenantId = toTenantId(msg.getTenantIdMSB(), msg.getTenantIdLSB());
        var entityId = EntityIdFactory.getByTypeAndUuid(msg.getEntityType(), new UUID(msg.getEntityIdMSB(), msg.getEntityIdLSB()));
        actorContext.tell(new CalculatedFieldTelemetryMsg(tenantId, entityId, msg, callback));
    }

    private void forwardToActorSystem(CalculatedFieldLinkedTelemetryMsgProto linkedMsg, TbCallback callback) {
        var msg = linkedMsg.getMsg();
        var tenantId = toTenantId(msg.getTenantIdMSB(), msg.getTenantIdLSB());
        var entityId = EntityIdFactory.getByTypeAndUuid(msg.getEntityType(), new UUID(msg.getEntityIdMSB(), msg.getEntityIdLSB()));
        actorContext.tell(new CalculatedFieldLinkedTelemetryMsg(tenantId, entityId, linkedMsg, callback));
    }

    private TenantId toTenantId(long tenantIdMSB, long tenantIdLSB) {
        return TenantId.fromUUID(new UUID(tenantIdMSB, tenantIdLSB));
    }

    @Override
    protected void stopConsumers() {
        super.stopConsumers();
        stateService.stop(); // eventConsumer will be stopped by stateService
    }

}
