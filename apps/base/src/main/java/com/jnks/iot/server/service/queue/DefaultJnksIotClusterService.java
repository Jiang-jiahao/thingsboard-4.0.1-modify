package com.jnks.iot.server.service.queue;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.cluster.JnksIotClusterService;
import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.HasName;
import com.jnks.iot.server.common.data.HasRuleEngineProfile;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.JnksIotResourceInfo;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.asset.Asset;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.AssetProfileId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.ToDeviceActorNotificationMsg;
import com.jnks.iot.server.common.msg.plugin.ComponentLifecycleMsg;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponse;
import com.jnks.iot.server.common.msg.rule.engine.DeviceNameOrTypeUpdateMsg;
import com.jnks.iot.server.common.util.ProtoUtils;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.ComponentLifecycleMsgProto;
import com.jnks.iot.server.gen.transport.TransportProtos.DeviceStateServiceMsgProto;
import com.jnks.iot.server.gen.transport.TransportProtos.EntityDeleteMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.FromDeviceRPCResponseProto;
import com.jnks.iot.server.gen.transport.TransportProtos.QueueDeleteMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.QueueUpdateMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ResourceDeleteMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ResourceUpdateMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.MultipleJnksIotQueueCallbackWrapper;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.common.ruleengine.JnksIotRuleEngineProducerService;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.TopicService;
import com.jnks.iot.server.queue.provider.JnksIotQueueProducerProvider;
import com.jnks.iot.server.service.gateway_device.GatewayNotificationsService;
import com.jnks.iot.server.service.ota.OtaPackageStateService;
import com.jnks.iot.server.service.profile.JnksIotAssetProfileCache;
import com.jnks.iot.server.service.profile.JnksIotDeviceProfileCache;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.jnks.iot.server.common.util.ProtoUtils.toProto;

/**
 * {@link JnksIotClusterService} 的默认实现，作为集群消息分发的统一门面。
 * <p>
 * 通过 {@link PartitionService} 解析目标分区，再经 {@link JnksIotQueueProducerProvider} 将消息投递到对应队列。
 * 涵盖向 Core、Rule Engine、Transport、Version Control、Calculated Field 等服务的推送，
 * 以及组件生命周期、队列变更的广播，和设备/租户配置变更通知。
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DefaultJnksIotClusterService implements JnksIotClusterService {

    /** 是否启用集群消息统计输出 */
    @Value("${cluster.stats.enabled:false}")
    private boolean statsEnabled;

    /** 发往 Core 的业务消息计数 */
    private final AtomicInteger toCoreMsgs = new AtomicInteger(0);
    /** 发往 Core 的通知消息计数 */
    private final AtomicInteger toCoreNfs = new AtomicInteger(0);
    /** 发往 Rule Engine 的业务消息计数 */
    private final AtomicInteger toRuleEngineMsgs = new AtomicInteger(0);
    /** 发往 Rule Engine 的通知消息计数 */
    private final AtomicInteger toRuleEngineNfs = new AtomicInteger(0);
    /** 发往 Transport 的通知消息计数 */
    private final AtomicInteger toTransportNfs = new AtomicInteger(0);

    /** 分区解析服务，用于确定消息目标 Topic 分区 */
    @Autowired
    @Lazy
    private PartitionService partitionService;

    /** 队列生产者提供者，按服务类型获取对应 Producer */
    @Autowired
    @Lazy
    private JnksIotQueueProducerProvider producerProvider;

    /** Rule Engine 专用生产者服务，封装 JnksIotMsg 发送逻辑 */
    @Autowired
    private JnksIotRuleEngineProducerService ruleEngineProducerService;

    /** OTA 包状态服务（可选，单体部署时可能不存在） */
    @Autowired(required = false)
    @Lazy
    private OtaPackageStateService otaPackageStateService;

    /** Topic 名称解析服务 */
    private final TopicService topicService;
    /** 设备配置缓存，用于解析 Rule Engine 路由 */
    private final JnksIotDeviceProfileCache deviceProfileCache;
    /** 资产配置缓存，用于解析 Rule Engine 路由 */
    private final JnksIotAssetProfileCache assetProfileCache;
    /** 网关设备通知服务（可选） */
    private final Optional<GatewayNotificationsService> gatewayNotificationsService;

    /**
     * 向 Core 服务推送业务消息。
     * 路由目标：根据 tenantId + entityId 解析 JNKS_IOT_CORE 分区。
     *
     * @param tenantId 租户 ID
     * @param entityId 实体 ID（用于分区哈希）
     * @param msg      Core 消息体
     * @param callback 发送完成回调
     */
    @Override
    public void pushMsgToCore(TenantId tenantId, EntityId entityId, ToCoreMsg msg, JnksIotQueueCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.JNKS_IOT_CORE, tenantId, entityId);
        producerProvider.getJnksIotCoreMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(UUID.randomUUID(), msg), callback);
        toCoreMsgs.incrementAndGet();
    }

    /**
     * 向指定 Core 分区推送业务消息（调用方已解析分区）。
     * 路由目标：传入的 TopicPartitionInfo。
     */
    @Override
    public void pushMsgToCore(TopicPartitionInfo tpi, UUID msgId, ToCoreMsg msg, JnksIotQueueCallback callback) {
        producerProvider.getJnksIotCoreMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(msgId, msg), callback);
        toCoreMsgs.incrementAndGet();
    }

    /**
     * 向 Core 推送设备 Actor 通知消息。
     * 路由目标：按 deviceId 解析 JNKS_IOT_CORE 分区。
     *
     * @param msg      设备 Actor 通知
     * @param callback 发送完成回调
     */
    @Override
    public void pushMsgToCore(ToDeviceActorNotificationMsg msg, JnksIotQueueCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.JNKS_IOT_CORE, msg.getTenantId(), msg.getDeviceId());
        log.trace("PUSHING msg: {} to:{}", msg, tpi);
        ToCoreMsg toCoreMsg = ToCoreMsg.newBuilder().setToDeviceActorNotification(toProto(msg)).build();
        producerProvider.getJnksIotCoreMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(msg.getDeviceId().getId(), toCoreMsg), callback);
        toCoreMsgs.incrementAndGet();
    }

    /**
     * 向所有 Core 实例广播通知消息。
     * 路由目标：每个 JNKS_IOT_CORE 服务实例的 notifications Topic。
     */
    @Override
    public void broadcastToCore(ToCoreNotificationMsg toCoreMsg) {
        UUID msgId = UUID.randomUUID();
        JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> toCoreNfProducer = producerProvider.getJnksIotCoreNotificationsMsgProducer();
        Set<String> jnksIotCoreServices = partitionService.getAllServiceIds(ServiceType.JNKS_IOT_CORE);
        for (String serviceId : jnksIotCoreServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_CORE, serviceId);
            toCoreNfProducer.send(tpi, new JnksIotProtoQueueMsg<>(msgId, toCoreMsg), null);
            toCoreNfs.incrementAndGet();
        }
    }

    /**
     * 向所有 Rule Engine 实例广播 Calculated Field 通知。
     * 路由目标：每个 JNKS_IOT_RULE_ENGINE 服务实例的 CF notifications Topic。
     */
    @Override
    public void broadcastToCalculatedFields(ToCalculatedFieldNotificationMsg toCfMsg, JnksIotQueueCallback callback) {
        UUID msgId = UUID.randomUUID();
        JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> toCfProducer = producerProvider.getCalculatedFieldsNotificationsMsgProducer();
        Set<String> jnksIotReServices = partitionService.getAllServiceIds(ServiceType.JNKS_IOT_RULE_ENGINE);
        MultipleJnksIotQueueCallbackWrapper callbackWrapper = new MultipleJnksIotQueueCallbackWrapper(jnksIotReServices.size(), callback);
        for (String serviceId : jnksIotReServices) {
            TopicPartitionInfo tpi = topicService.getCalculatedFieldNotificationsTopic(serviceId);
            toCfProducer.send(tpi, new JnksIotProtoQueueMsg<>(msgId, toCfMsg), callbackWrapper);
            toRuleEngineNfs.incrementAndGet();
        }
    }

    /**
     * 向 Version Control 服务推送消息。
     * 路由目标：JNKS_IOT_VC_EXECUTOR 分区（按 tenantId 哈希）。
     */
    @Override
    public void pushMsgToVersionControl(TenantId tenantId, ToVersionControlServiceMsg msg, JnksIotQueueCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.JNKS_IOT_VC_EXECUTOR, TenantId.SYS_TENANT_ID, tenantId);
        log.trace("PUSHING msg: {} to:{}", msg, tpi);
        producerProvider.getJnksIotVersionControlMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(tenantId.getId(), msg), callback);
        //TODO: ashvayka
        toCoreMsgs.incrementAndGet();
    }

    /**
     * 向指定 Core 实例推送设备 RPC 响应通知。
     * 路由目标：指定 serviceId 的 JNKS_IOT_CORE notifications Topic。
     */
    @Override
    public void pushNotificationToCore(String serviceId, FromDeviceRpcResponse response, JnksIotQueueCallback callback) {
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_CORE, serviceId);
        log.trace("PUSHING msg: {} to:{}", response, tpi);
        FromDeviceRPCResponseProto.Builder builder = FromDeviceRPCResponseProto.newBuilder()
                .setRequestIdMSB(response.getId().getMostSignificantBits())
                .setRequestIdLSB(response.getId().getLeastSignificantBits())
                .setError(response.getError().isPresent() ? response.getError().get().ordinal() : -1);
        response.getResponse().ifPresent(builder::setResponse);
        ToCoreNotificationMsg msg = ToCoreNotificationMsg.newBuilder().setFromDeviceRpcResponse(builder).build();
        producerProvider.getJnksIotCoreNotificationsMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(response.getId(), msg), callback);
        toCoreNfs.incrementAndGet();
    }

    /**
     * 向指定 Core 实例推送 REST API 调用响应通知。
     * 路由目标：指定 targetServiceId 的 JNKS_IOT_CORE notifications Topic。
     */
    @Override
    public void pushNotificationToCore(String targetServiceId, TransportProtos.RestApiCallResponseMsgProto responseMsgProto, JnksIotQueueCallback callback) {
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_CORE, targetServiceId);
        ToCoreNotificationMsg msg = ToCoreNotificationMsg.newBuilder().setRestApiCallResponseMsg(responseMsgProto).build();
        producerProvider.getJnksIotCoreNotificationsMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(UUID.randomUUID(), msg), callback);
        toCoreNfs.incrementAndGet();
    }

    /**
     * 向指定 Rule Engine 分区推送业务消息（调用方已解析分区）。
     * 路由目标：传入的 TopicPartitionInfo。
     */
    @Override
    public void pushMsgToRuleEngine(TopicPartitionInfo tpi, UUID msgId, ToRuleEngineMsg msg, JnksIotQueueCallback callback) {
        log.trace("PUSHING msg: {} to:{}", msg, tpi);
        producerProvider.getRuleEngineMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(msgId, msg), callback);
        toRuleEngineMsgs.incrementAndGet();
    }

    /**
     * 向 Rule Engine 推送 JnksIotMsg（使用实体 Profile 中的默认队列与规则链）。
     * 路由目标：由 ruleEngineProducerService 按 tenantId + JnksIotMsg 解析 JNKS_IOT_RULE_ENGINE 分区。
     */
    @Override
    public void pushMsgToRuleEngine(TenantId tenantId, EntityId entityId, JnksIotMsg jnksIotMsg, JnksIotQueueCallback callback) {
        pushMsgToRuleEngine(tenantId, entityId, jnksIotMsg, false, callback);
    }

    /**
     * 向 Rule Engine 推送 JnksIotMsg，可选是否沿用 JnksIotMsg 自带的队列名。
     * 路由目标：由 ruleEngineProducerService 按 tenantId + JnksIotMsg 解析 JNKS_IOT_RULE_ENGINE 分区。
     */
    @Override
    public void pushMsgToRuleEngine(TenantId tenantId, EntityId entityId, JnksIotMsg jnksIotMsg, boolean useQueueFromJnksIotMsg, JnksIotQueueCallback callback) {
        if (tenantId == null || tenantId.isNullUid()) {
            if (entityId.getEntityType().equals(EntityType.TENANT)) {
                tenantId = TenantId.fromUUID(entityId.getId());
            } else {
                log.warn("[{}][{}] Received invalid message: {}", tenantId, entityId, jnksIotMsg);
                return;
            }
        } else {
            HasRuleEngineProfile ruleEngineProfile = getRuleEngineProfileForEntityOrElseNull(tenantId, entityId, jnksIotMsg);
            jnksIotMsg = transformMsg(jnksIotMsg, ruleEngineProfile, useQueueFromJnksIotMsg);
        }
        ruleEngineProducerService.sendToRuleEngine(producerProvider.getRuleEngineMsgProducer(), tenantId, jnksIotMsg, callback);
        toRuleEngineMsgs.incrementAndGet();
    }

    /**
     * 根据实体类型获取 Rule Engine Profile（默认规则链与队列）。
     * 删除事件时从消息体反序列化实体以获取 Profile ID。
     */
    HasRuleEngineProfile getRuleEngineProfileForEntityOrElseNull(TenantId tenantId, EntityId entityId, JnksIotMsg jnksIotMsg) {
        if (entityId.getEntityType().equals(EntityType.DEVICE)) {
            if (JnksIotMsgType.ENTITY_DELETED.equals(jnksIotMsg.getInternalType())) {
                try {
                    Device deletedDevice = JacksonUtil.fromString(jnksIotMsg.getData(), Device.class);
                    if (deletedDevice == null) {
                        return null;
                    }
                    return deviceProfileCache.get(tenantId, deletedDevice.getDeviceProfileId());
                } catch (Exception e) {
                    log.warn("[{}][{}] Failed to deserialize device: {}", tenantId, entityId, jnksIotMsg, e);
                    return null;
                }
            } else {
                return deviceProfileCache.get(tenantId, new DeviceId(entityId.getId()));
            }
        } else if (entityId.getEntityType().equals(EntityType.DEVICE_PROFILE)) {
            return deviceProfileCache.get(tenantId, new DeviceProfileId(entityId.getId()));
        } else if (entityId.getEntityType().equals(EntityType.ASSET)) {
            if (JnksIotMsgType.ENTITY_DELETED.equals(jnksIotMsg.getInternalType())) {
                try {
                    Asset deletedAsset = JacksonUtil.fromString(jnksIotMsg.getData(), Asset.class);
                    if (deletedAsset == null) {
                        return null;
                    }
                    return assetProfileCache.get(tenantId, deletedAsset.getAssetProfileId());
                } catch (Exception e) {
                    log.warn("[{}][{}] Failed to deserialize asset: {}", tenantId, entityId, jnksIotMsg, e);
                    return null;
                }
            } else {
                return assetProfileCache.get(tenantId, new AssetId(entityId.getId()));
            }
        } else if (entityId.getEntityType().equals(EntityType.ASSET_PROFILE)) {
            return assetProfileCache.get(tenantId, new AssetProfileId(entityId.getId()));
        }
        return null;
    }

    /**
     * 按 Profile 将 JnksIotMsg 重定向到默认规则链和/或默认队列。
     */
    private JnksIotMsg transformMsg(JnksIotMsg jnksIotMsg, HasRuleEngineProfile ruleEngineProfile, boolean useQueueFromJnksIotMsg) {
        if (ruleEngineProfile != null) {
            RuleChainId targetRuleChainId = ruleEngineProfile.getDefaultRuleChainId();
            String targetQueueName = useQueueFromJnksIotMsg ? jnksIotMsg.getQueueName() : ruleEngineProfile.getDefaultQueueName();

            boolean isRuleChainTransform = targetRuleChainId != null && !targetRuleChainId.equals(jnksIotMsg.getRuleChainId());
            boolean isQueueTransform = targetQueueName != null && !targetQueueName.equals(jnksIotMsg.getQueueName());

            if (isRuleChainTransform && isQueueTransform) {
                jnksIotMsg = jnksIotMsg.transform()
                        .queueName(targetQueueName)
                        .ruleChainId(targetRuleChainId)
                        .build();
            } else if (isRuleChainTransform) {
                jnksIotMsg = jnksIotMsg.transform()
                        .ruleChainId(targetRuleChainId)
                        .build();
            } else if (isQueueTransform) {
                jnksIotMsg = jnksIotMsg.transform(targetQueueName);
            }
        }
        return jnksIotMsg;
    }

    /**
     * 向指定 Rule Engine 实例推送设备 RPC 响应通知。
     * 路由目标：指定 serviceId 的 JNKS_IOT_RULE_ENGINE notifications Topic。
     */
    @Override
    public void pushNotificationToRuleEngine(String serviceId, FromDeviceRpcResponse response, JnksIotQueueCallback callback) {
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_RULE_ENGINE, serviceId);
        log.trace("PUSHING msg: {} to:{}", response, tpi);
        FromDeviceRPCResponseProto.Builder builder = FromDeviceRPCResponseProto.newBuilder()
                .setRequestIdMSB(response.getId().getMostSignificantBits())
                .setRequestIdLSB(response.getId().getLeastSignificantBits())
                .setError(response.getError().isPresent() ? response.getError().get().ordinal() : -1);
        response.getResponse().ifPresent(builder::setResponse);
        ToRuleEngineNotificationMsg msg = ToRuleEngineNotificationMsg.newBuilder().setFromDeviceRpcResponse(builder).build();
        producerProvider.getRuleEngineNotificationsMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(response.getId(), msg), callback);
        toRuleEngineNfs.incrementAndGet();
    }

    /**
     * 向指定 Transport 实例推送通知消息。
     * 路由目标：指定 serviceId 的 JNKS_IOT_TRANSPORT notifications Topic。
     */
    @Override
    public void pushNotificationToTransport(String serviceId, ToTransportMsg response, JnksIotQueueCallback callback) {
        if (serviceId == null || serviceId.isEmpty()) {
            log.trace("pushNotificationToTransport: skipping message without serviceId [{}], (ToTransportMsg) response [{}]", serviceId, response);
            if (callback != null) {
                callback.onSuccess(null); // 无有效 serviceId 时视为已发送，回调无有效载荷
            }
            return;
        }
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_TRANSPORT, serviceId);
        log.trace("PUSHING msg: {} to:{}", response, tpi);
        producerProvider.getTransportNotificationsMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(UUID.randomUUID(), response), callback);
        toTransportNfs.incrementAndGet();
    }

    /**
     * 向 Calculated Field 队列推送消息。
     * 路由目标：JNKS_IOT_RULE_ENGINE 的 CF 专用队列分区。
     */
    @Override
    public void pushMsgToCalculatedFields(TenantId tenantId, EntityId entityId, ToCalculatedFieldMsg msg, JnksIotQueueCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.JNKS_IOT_RULE_ENGINE, DataConstants.CF_QUEUE_NAME, tenantId, entityId);
        pushMsgToCalculatedFields(tpi, UUID.randomUUID(), msg, callback);
    }

    /**
     * 向指定 Calculated Field 分区推送消息（调用方已解析分区）。
     * 路由目标：传入的 TopicPartitionInfo。
     */
    @Override
    public void pushMsgToCalculatedFields(TopicPartitionInfo tpi, UUID msgId, ToCalculatedFieldMsg msg, JnksIotQueueCallback callback) {
        log.trace("PUSHING msg: {} to:{}", msg, tpi);
        producerProvider.getCalculatedFieldsMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(msgId, msg), callback);
        toRuleEngineMsgs.incrementAndGet(); // TODO: 待 ServiceType.CALCULATED_FIELDS 独立后增加单独计数器
    }

    /**
     * 广播实体生命周期状态变更事件（Core + Rule Engine）。
     *
     * @param tenantId 租户 ID
     * @param entityId 实体 ID
     * @param state    生命周期事件类型
     */
    @Override
    public void broadcastEntityStateChangeEvent(TenantId tenantId, EntityId entityId, ComponentLifecycleEvent state) {
        log.trace("[{}] Processing {} state change event: {}", tenantId, entityId.getEntityType(), state);
        broadcast(new ComponentLifecycleMsg(tenantId, entityId, state));
    }

    /**
     * 设备配置变更：通知 Transport、广播生命周期，并更新 OTA 状态。
     */
    @Override
    public void onDeviceProfileChange(DeviceProfile deviceProfile, DeviceProfile oldDeviceProfile, JnksIotQueueCallback callback) {
        boolean isFirmwareChanged = false;
        boolean isSoftwareChanged = false;
        if (oldDeviceProfile != null) {
            isFirmwareChanged = !Objects.equals(deviceProfile.getFirmwareId(), oldDeviceProfile.getFirmwareId());
            isSoftwareChanged = !Objects.equals(deviceProfile.getSoftwareId(), oldDeviceProfile.getSoftwareId());
        }
        broadcastEntityChangeToTransport(deviceProfile.getTenantId(), deviceProfile.getId(), deviceProfile, callback);
        broadcastEntityStateChangeEvent(deviceProfile.getTenantId(), deviceProfile.getId(),
                oldDeviceProfile == null ? ComponentLifecycleEvent.CREATED : ComponentLifecycleEvent.UPDATED);
        if (otaPackageStateService != null) {
            otaPackageStateService.update(deviceProfile, isFirmwareChanged, isSoftwareChanged);
        }
    }

    /** 租户配置变更：广播 EntityUpdateMsg 至 Transport。 */
    @Override
    public void onTenantProfileChange(TenantProfile tenantProfile, JnksIotQueueCallback callback) {
        broadcastEntityChangeToTransport(TenantId.SYS_TENANT_ID, tenantProfile.getId(), tenantProfile, callback);
    }

    /** 租户变更：广播 EntityUpdateMsg 至 Transport。 */
    @Override
    public void onTenantChange(Tenant tenant, JnksIotQueueCallback callback) {
        broadcastEntityChangeToTransport(TenantId.SYS_TENANT_ID, tenant.getId(), tenant, callback);
    }

    /** API 用量状态变更：通知 Transport 并广播生命周期事件。 */
    @Override
    public void onApiStateChange(ApiUsageState apiUsageState, JnksIotQueueCallback callback) {
        broadcastEntityChangeToTransport(apiUsageState.getTenantId(), apiUsageState.getId(), apiUsageState, callback);
        broadcast(new ComponentLifecycleMsg(apiUsageState.getTenantId(), apiUsageState.getId(), ComponentLifecycleEvent.UPDATED));
    }

    /** 设备配置删除：广播 EntityDeleteMsg 至 Transport。 */
    @Override
    public void onDeviceProfileDelete(DeviceProfile entity, JnksIotQueueCallback callback) {
        broadcastEntityDeleteToTransport(entity.getTenantId(), entity.getId(), entity.getName(), callback);
    }

    /** 租户配置删除：广播 EntityDeleteMsg 至 Transport。 */
    @Override
    public void onTenantProfileDelete(TenantProfile entity, JnksIotQueueCallback callback) {
        broadcastEntityDeleteToTransport(TenantId.SYS_TENANT_ID, entity.getId(), entity.getName(), callback);
    }

    /** 租户删除：广播 EntityDeleteMsg 至 Transport。 */
    @Override
    public void onTenantDelete(Tenant entity, JnksIotQueueCallback callback) {
        broadcastEntityDeleteToTransport(TenantId.SYS_TENANT_ID, entity.getId(), entity.getName(), callback);
    }

    /** 设备删除：通知网关、Transport、设备状态服务，并广播生命周期 DELETED 事件。 */
    @Override
    public void onDeviceDeleted(TenantId tenantId, Device device, JnksIotQueueCallback callback) {
        DeviceId deviceId = device.getId();
        gatewayNotificationsService.ifPresent(s -> s.onDeviceDeleted(device));
        broadcastEntityDeleteToTransport(tenantId, deviceId, device.getName(), callback);
        sendDeviceStateServiceEvent(tenantId, deviceId, false, false, true);
        broadcastEntityStateChangeEvent(tenantId, deviceId, ComponentLifecycleEvent.DELETED);
    }

    /** 资产删除：广播生命周期 DELETED 事件。 */
    @Override
    public void onAssetDeleted(TenantId tenantId, Asset asset, JnksIotQueueCallback callback) {
        AssetId assetId = asset.getId();
        broadcastEntityStateChangeEvent(tenantId, assetId, ComponentLifecycleEvent.DELETED);
    }

    /** 设备跨租户分配：在旧租户侧执行删除流程，在新租户侧注册设备状态。 */
    @Override
    public void onDeviceAssignedToTenant(TenantId oldTenantId, Device device) {
        onDeviceDeleted(oldTenantId, device, null);
        sendDeviceStateServiceEvent(device.getTenantId(), device.getId(), true, false, false);
    }

    /** LWM2M 模型资源变更：广播 ResourceUpdateMsg 至 LWM2M Transport 实例。 */
    @Override
    public void onResourceChange(JnksIotResourceInfo resource, JnksIotQueueCallback callback) {
        if (resource.getResourceType() == ResourceType.LWM2M_MODEL) {
            TenantId tenantId = resource.getTenantId();
            log.trace("[{}][{}][{}] Processing change resource", tenantId, resource.getResourceType(), resource.getResourceKey());
            ResourceUpdateMsg resourceUpdateMsg = ResourceUpdateMsg.newBuilder()
                    .setTenantIdMSB(tenantId.getId().getMostSignificantBits())
                    .setTenantIdLSB(tenantId.getId().getLeastSignificantBits())
                    .setResourceType(resource.getResourceType().name())
                    .setResourceKey(resource.getResourceKey())
                    .build();
            ToTransportMsg transportMsg = ToTransportMsg.newBuilder().setResourceUpdateMsg(resourceUpdateMsg).build();
            broadcast(transportMsg, DataConstants.LWM2M_TRANSPORT_NAME, callback);
        }
    }

    /** LWM2M 模型资源删除：广播 ResourceDeleteMsg 至 LWM2M Transport 实例。 */
    @Override
    public void onResourceDeleted(JnksIotResourceInfo resource, JnksIotQueueCallback callback) {
        if (resource.getResourceType() == ResourceType.LWM2M_MODEL) {
            log.trace("[{}][{}][{}] Processing delete resource", resource.getTenantId(), resource.getResourceType(), resource.getResourceKey());
            ResourceDeleteMsg resourceDeleteMsg = ResourceDeleteMsg.newBuilder()
                    .setTenantIdMSB(resource.getTenantId().getId().getMostSignificantBits())
                    .setTenantIdLSB(resource.getTenantId().getId().getLeastSignificantBits())
                    .setResourceType(resource.getResourceType().name())
                    .setResourceKey(resource.getResourceKey())
                    .build();
            ToTransportMsg transportMsg = ToTransportMsg.newBuilder().setResourceDeleteMsg(resourceDeleteMsg).build();
            broadcast(transportMsg, DataConstants.LWM2M_TRANSPORT_NAME, callback);
        }
    }

    /**
     * 广播实体变更（EntityUpdateMsg）至所有 Transport 实例。
     *
     * @param tenantId 租户 ID
     * @param entityid 实体 ID
     * @param entity   变更后的实体对象
     * @param callback 发送完成回调
     */
    private <T> void broadcastEntityChangeToTransport(TenantId tenantId, EntityId entityid, T entity, JnksIotQueueCallback callback) {
        String entityName = (entity instanceof HasName) ? ((HasName) entity).getName() : entity.getClass().getName();
        log.trace("[{}][{}][{}] Processing [{}] change event", tenantId, entityid.getEntityType(), entityid.getId(), entityName);
        ToTransportMsg transportMsg = ToTransportMsg.newBuilder().setEntityUpdateMsg(ProtoUtils.toEntityUpdateProto(entity)).build();
        broadcast(transportMsg, callback);
    }

    /**
     * 广播实体删除（EntityDeleteMsg）至所有 Transport 实例。
     *
     * @param tenantId 租户 ID
     * @param entityId 实体 ID
     * @param name     实体名称
     * @param callback 发送完成回调
     */
    private void broadcastEntityDeleteToTransport(TenantId tenantId, EntityId entityId, String name, JnksIotQueueCallback callback) {
        log.trace("[{}][{}][{}] Processing [{}] delete event", tenantId, entityId.getEntityType(), entityId.getId(), name);
        EntityDeleteMsg entityDeleteMsg = EntityDeleteMsg.newBuilder()
                .setEntityType(entityId.getEntityType().name())
                .setEntityIdMSB(entityId.getId().getMostSignificantBits())
                .setEntityIdLSB(entityId.getId().getLeastSignificantBits())
                .build();
        ToTransportMsg transportMsg = ToTransportMsg.newBuilder().setEntityDeleteMsg(entityDeleteMsg).build();
        broadcast(transportMsg, callback);
    }

    /**
     * 向所有 Transport 实例广播通知消息。
     */
    private void broadcast(ToTransportMsg transportMsg, JnksIotQueueCallback callback) {
        Set<String> jnksIotTransportServices = partitionService.getAllServiceIds(ServiceType.JNKS_IOT_TRANSPORT);
        broadcast(transportMsg, jnksIotTransportServices, callback);
    }

    /**
     * 向支持指定 Transport 类型的实例广播通知消息。
     */
    private void broadcast(ToTransportMsg transportMsg, String transportType, JnksIotQueueCallback callback) {
        Set<String> jnksIotTransportServices = partitionService.getAllServices(ServiceType.JNKS_IOT_TRANSPORT).stream()
                .filter(info -> info.getTransportsList().contains(transportType))
                .map(TransportProtos.ServiceInfo::getServiceId).collect(Collectors.toSet());
        broadcast(transportMsg, jnksIotTransportServices, callback);
    }

    /**
     * 向给定 Transport 服务 ID 集合广播通知消息。
     *
     * @param transportMsg          Transport 消息体
     * @param jnksIotTransportServices   目标 Transport 服务 ID 集合
     * @param callback              发送完成回调（多实例时用 MultipleJnksIotQueueCallbackWrapper 聚合）
     */
    private void broadcast(ToTransportMsg transportMsg, Set<String> jnksIotTransportServices, JnksIotQueueCallback callback) {
        JnksIotQueueProducer<JnksIotProtoQueueMsg<ToTransportMsg>> toTransportNfProducer = producerProvider.getTransportNotificationsMsgProducer();
        JnksIotQueueCallback proxyCallback = callback != null ? new MultipleJnksIotQueueCallbackWrapper(jnksIotTransportServices.size(), callback) : null;
        for (String transportServiceId : jnksIotTransportServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_TRANSPORT, transportServiceId);
            toTransportNfProducer.send(tpi, new JnksIotProtoQueueMsg<>(UUID.randomUUID(), transportMsg), proxyCallback);
            toTransportNfs.incrementAndGet();
        }
    }

    /**
     * 广播组件生命周期消息。
     * <p>
     * 对租户/配置/设备等特定实体类型，同时通知 Core 与 Rule Engine；
     * 单体部署时 Core 与 Rule Engine 共用同一 serviceId，需 removeAll 避免重复投递。
     *
     * @param msg 组件生命周期消息
     */
    private void broadcast(ComponentLifecycleMsg msg) {
        ComponentLifecycleMsgProto componentLifecycleMsgProto = toProto(msg);
        JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> toRuleEngineProducer = producerProvider.getRuleEngineNotificationsMsgProducer();
        Set<String> jnksIotRuleEngineServices = partitionService.getAllServiceIds(ServiceType.JNKS_IOT_RULE_ENGINE);
        EntityType entityType = msg.getEntityId().getEntityType();
        if (entityType.equals(EntityType.TENANT)
                || entityType.equals(EntityType.TENANT_PROFILE)
                || entityType.equals(EntityType.DEVICE_PROFILE)
                || (entityType.equals(EntityType.ASSET) && msg.getEvent() == ComponentLifecycleEvent.UPDATED)
                || entityType.equals(EntityType.ASSET_PROFILE)
                || entityType.equals(EntityType.API_USAGE_STATE)
                || (entityType.equals(EntityType.DEVICE) && msg.getEvent() == ComponentLifecycleEvent.UPDATED)
                || entityType.equals(EntityType.ENTITY_VIEW)
                || entityType.equals(EntityType.NOTIFICATION_RULE)
                || entityType.equals(EntityType.CALCULATED_FIELD)
        ) {
            // 同时广播至 Core 与 Rule Engine
            JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> toCoreNfProducer = producerProvider.getJnksIotCoreNotificationsMsgProducer();
            Set<String> jnksIotCoreServices = partitionService.getAllServiceIds(ServiceType.JNKS_IOT_CORE);
            for (String serviceId : jnksIotCoreServices) {
                TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_CORE, serviceId);
                ToCoreNotificationMsg toCoreMsg = ToCoreNotificationMsg.newBuilder().setComponentLifecycle(componentLifecycleMsgProto).build();
                toCoreNfProducer.send(tpi, new JnksIotProtoQueueMsg<>(msg.getEntityId().getId(), toCoreMsg), null);
                toCoreNfs.incrementAndGet();
            }
            // 单体模式下 Core 与 RE 共用 serviceId，从 RE 集合中移除以避免重复通知
            jnksIotRuleEngineServices.removeAll(jnksIotCoreServices);
        }
        // 广播至剩余 Rule Engine 实例
        for (String serviceId : jnksIotRuleEngineServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_RULE_ENGINE, serviceId);
            ToRuleEngineNotificationMsg toRuleEngineMsg = ToRuleEngineNotificationMsg.newBuilder().setComponentLifecycle(componentLifecycleMsgProto).build();
            toRuleEngineProducer.send(tpi, new JnksIotProtoQueueMsg<>(msg.getEntityId().getId(), toRuleEngineMsg), null);
            toRuleEngineNfs.incrementAndGet();
        }
    }

    /** 定时打印并重置各队列消息计数（需 statsEnabled=true）。 */
    @Scheduled(fixedDelayString = "${cluster.stats.print_interval_ms}")
    public void printStats() {
        if (statsEnabled) {
            int toCoreMsgCnt = toCoreMsgs.getAndSet(0);
            int toCoreNfsCnt = toCoreNfs.getAndSet(0);
            int toRuleEngineMsgsCnt = toRuleEngineMsgs.getAndSet(0);
            int toRuleEngineNfsCnt = toRuleEngineNfs.getAndSet(0);
            int toTransportNfsCnt = toTransportNfs.getAndSet(0);
            if (toCoreMsgCnt > 0 || toCoreNfsCnt > 0 || toRuleEngineMsgsCnt > 0 || toRuleEngineNfsCnt > 0 || toTransportNfsCnt > 0) {
                log.info("To JnksIotCore: [{}] messages [{}] notifications; To JnksIotRuleEngine: [{}] messages [{}] notifications; To Transport: [{}] notifications",
                        toCoreMsgCnt, toCoreNfsCnt, toRuleEngineMsgsCnt, toRuleEngineNfsCnt, toTransportNfsCnt);
            }
        }
    }

    /**
     * 向 Core 推送设备状态服务事件（新增/更新/删除）。
     */
    private void sendDeviceStateServiceEvent(TenantId tenantId, DeviceId deviceId, boolean added, boolean updated, boolean deleted) {
        DeviceStateServiceMsgProto.Builder builder = DeviceStateServiceMsgProto.newBuilder();
        builder.setTenantIdMSB(tenantId.getId().getMostSignificantBits());
        builder.setTenantIdLSB(tenantId.getId().getLeastSignificantBits());
        builder.setDeviceIdMSB(deviceId.getId().getMostSignificantBits());
        builder.setDeviceIdLSB(deviceId.getId().getLeastSignificantBits());
        builder.setAdded(added);
        builder.setUpdated(updated);
        builder.setDeleted(deleted);
        DeviceStateServiceMsgProto msg = builder.build();
        pushMsgToCore(tenantId, deviceId, ToCoreMsg.newBuilder().setDeviceStateServiceMsg(msg).build(), null);
    }

    /**
     * 设备创建或更新：通知 Transport、网关、Core Actor，并广播生命周期事件。
     *
     * @param entity 当前设备
     * @param old    更新前的设备（创建时为 null）
     */
    @Override
    public void onDeviceUpdated(Device entity, Device old) {
        var created = old == null;
        // 设备实体变更需同步至 Transport，以便连接层刷新本地缓存
        broadcastEntityChangeToTransport(entity.getTenantId(), entity.getId(), entity, null);
        var msg = ComponentLifecycleMsg.builder()
                .tenantId(entity.getTenantId())
                .entityId(entity.getId())
                .profileId(entity.getDeviceProfileId())
                .name(entity.getName());
        if (created) {
            msg.event(ComponentLifecycleEvent.CREATED);
        } else {
            boolean deviceNameChanged = !entity.getName().equals(old.getName());
            if (deviceNameChanged) {
                gatewayNotificationsService.ifPresent(s -> s.onDeviceUpdated(entity, old));
            }
            boolean deviceProfileChanged = !entity.getDeviceProfileId().equals(old.getDeviceProfileId());
            if (deviceNameChanged || deviceProfileChanged) {
                // 名称或类型变更时通知 Core 设备 Actor 刷新元数据
                pushMsgToCore(new DeviceNameOrTypeUpdateMsg(entity.getTenantId(), entity.getId(), entity.getName(), entity.getType()), null);
            }
            msg.event(ComponentLifecycleEvent.UPDATED)
                    .oldProfileId(old.getDeviceProfileId())
                    .oldName(old.getName());
        }
        broadcast(msg.build());
        sendDeviceStateServiceEvent(entity.getTenantId(), entity.getId(), created, !created, false);
        if (otaPackageStateService != null) {
            otaPackageStateService.update(entity, old);
        }
    }

    /** 资产创建或更新：广播生命周期 CREATED/UPDATED 事件。 */
    @Override
    public void onAssetUpdated(Asset entity, Asset old) {
        var created = old == null;
        var msg = ComponentLifecycleMsg.builder()
                .tenantId(entity.getTenantId())
                .entityId(entity.getId())
                .profileId(entity.getAssetProfileId())
                .name(entity.getName());
        if (created) {
            msg.event(ComponentLifecycleEvent.CREATED);
        } else {
            msg.event(ComponentLifecycleEvent.UPDATED)
                    .oldProfileId(old.getAssetProfileId())
                    .oldName(old.getName());
        }
        broadcast(msg.build());
    }

    /** 计算字段创建或更新：广播生命周期事件。 */
    @Override
    public void onCalculatedFieldUpdated(CalculatedField calculatedField, CalculatedField oldCalculatedField, JnksIotQueueCallback callback) {
        broadcastEntityStateChangeEvent(calculatedField.getTenantId(), calculatedField.getId(), oldCalculatedField == null ? ComponentLifecycleEvent.CREATED : ComponentLifecycleEvent.UPDATED);
    }

    /** 计算字段删除：广播生命周期 DELETED 事件。 */
    @Override
    public void onCalculatedFieldDeleted(CalculatedField calculatedField, JnksIotQueueCallback callback) {
        broadcastEntityStateChangeEvent(calculatedField.getTenantId(), calculatedField.getId(), ComponentLifecycleEvent.DELETED);
    }

    /**
     * 队列配置更新：广播 QueueUpdateMsg 至 Rule Engine、Core、Transport。
     */
    @Override
    public void onQueuesUpdate(List<Queue> queues) {
        List<QueueUpdateMsg> queueUpdateMsgs = queues.stream()
                .map(queue -> QueueUpdateMsg.newBuilder()
                        .setTenantIdMSB(queue.getTenantId().getId().getMostSignificantBits())
                        .setTenantIdLSB(queue.getTenantId().getId().getLeastSignificantBits())
                        .setQueueIdMSB(queue.getId().getId().getMostSignificantBits())
                        .setQueueIdLSB(queue.getId().getId().getLeastSignificantBits())
                        .setQueueName(queue.getName())
                        .setQueueTopic(queue.getTopic())
                        .setPartitions(queue.getPartitions())
                        .setDuplicateMsgToAllPartitions(queue.isDuplicateMsgToAllPartitions())
                        .build())
                .collect(Collectors.toList());

        ToRuleEngineNotificationMsg ruleEngineMsg = ToRuleEngineNotificationMsg.newBuilder().addAllQueueUpdateMsgs(queueUpdateMsgs).build();
        ToCoreNotificationMsg coreMsg = ToCoreNotificationMsg.newBuilder().addAllQueueUpdateMsgs(queueUpdateMsgs).build();
        ToTransportMsg transportMsg = ToTransportMsg.newBuilder().addAllQueueUpdateMsgs(queueUpdateMsgs).build();
        doSendQueueNotifications(ruleEngineMsg, coreMsg, transportMsg);
    }

    /**
     * 队列配置删除：广播 QueueDeleteMsg 至 Rule Engine、Core、Transport。
     */
    @Override
    public void onQueuesDelete(List<Queue> queues) {
        List<QueueDeleteMsg> queueDeleteMsgs = queues.stream()
                .map(queue -> QueueDeleteMsg.newBuilder()
                        .setTenantIdMSB(queue.getTenantId().getId().getMostSignificantBits())
                        .setTenantIdLSB(queue.getTenantId().getId().getLeastSignificantBits())
                        .setQueueIdMSB(queue.getId().getId().getMostSignificantBits())
                        .setQueueIdLSB(queue.getId().getId().getLeastSignificantBits())
                        .setQueueName(queue.getName())
                        .build())
                .collect(Collectors.toList());

        ToRuleEngineNotificationMsg ruleEngineMsg = ToRuleEngineNotificationMsg.newBuilder().addAllQueueDeleteMsgs(queueDeleteMsgs).build();
        ToCoreNotificationMsg coreMsg = ToCoreNotificationMsg.newBuilder().addAllQueueDeleteMsgs(queueDeleteMsgs).build();
        ToTransportMsg transportMsg = ToTransportMsg.newBuilder().addAllQueueDeleteMsgs(queueDeleteMsgs).build();
        doSendQueueNotifications(ruleEngineMsg, coreMsg, transportMsg);
    }

    /**
     * 分发队列变更通知，removeAll 避免单体部署下同一 serviceId 重复投递。
     */
    private void doSendQueueNotifications(ToRuleEngineNotificationMsg ruleEngineMsg, ToCoreNotificationMsg coreMsg, ToTransportMsg transportMsg) {
        Set<String> jnksIotRuleEngineServices = partitionService.getAllServiceIds(ServiceType.JNKS_IOT_RULE_ENGINE);
        Set<String> jnksIotCoreServices = partitionService.getAllServiceIds(ServiceType.JNKS_IOT_CORE);
        Set<String> jnksIotTransportServices = partitionService.getAllServiceIds(ServiceType.JNKS_IOT_TRANSPORT);
        // 单体模式下各服务共用 serviceId，去重以避免重复推送
        jnksIotTransportServices.removeAll(jnksIotCoreServices);
        jnksIotCoreServices.removeAll(jnksIotRuleEngineServices);

        for (String ruleEngineServiceId : jnksIotRuleEngineServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_RULE_ENGINE, ruleEngineServiceId);
            producerProvider.getRuleEngineNotificationsMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(UUID.randomUUID(), ruleEngineMsg), null);
            toRuleEngineNfs.incrementAndGet();
        }
        for (String coreServiceId : jnksIotCoreServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_CORE, coreServiceId);
            producerProvider.getJnksIotCoreNotificationsMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(UUID.randomUUID(), coreMsg), null);
            toCoreNfs.incrementAndGet();
        }
        for (String transportServiceId : jnksIotTransportServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.JNKS_IOT_TRANSPORT, transportServiceId);
            producerProvider.getTransportNotificationsMsgProducer().send(tpi, new JnksIotProtoQueueMsg<>(UUID.randomUUID(), transportMsg), null);
            toTransportNfs.incrementAndGet();
        }
    }

}
