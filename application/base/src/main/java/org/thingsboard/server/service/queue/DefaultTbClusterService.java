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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.cache.TbTransactionalCache;
import org.thingsboard.server.cluster.TbClusterService;
import org.thingsboard.server.common.data.ApiUsageState;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.EdgeUtils;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.HasName;
import org.thingsboard.server.common.data.HasRuleEngineProfile;
import org.thingsboard.server.common.data.ResourceType;
import org.thingsboard.server.common.data.TbResourceInfo;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.cf.CalculatedField;
import org.thingsboard.server.common.data.edge.EdgeEventActionType;
import org.thingsboard.server.common.data.edge.EdgeEventType;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.AssetProfileId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.EdgeId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.RuleChainId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.msg.TbMsgType;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.data.queue.Queue;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.ToDeviceActorNotificationMsg;
import org.thingsboard.server.common.msg.edge.EdgeEventUpdateMsg;
import org.thingsboard.server.common.msg.edge.EdgeHighPriorityMsg;
import org.thingsboard.server.common.msg.edge.FromEdgeSyncResponse;
import org.thingsboard.server.common.msg.edge.ToEdgeSyncRequest;
import org.thingsboard.server.common.msg.plugin.ComponentLifecycleMsg;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.common.msg.rpc.FromDeviceRpcResponse;
import org.thingsboard.server.common.msg.rule.engine.DeviceEdgeUpdateMsg;
import org.thingsboard.server.common.msg.rule.engine.DeviceNameOrTypeUpdateMsg;
import org.thingsboard.server.common.util.ProtoUtils;
import org.thingsboard.server.dao.edge.EdgeService;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.ComponentLifecycleMsgProto;
import org.thingsboard.server.gen.transport.TransportProtos.DeviceStateServiceMsgProto;
import org.thingsboard.server.gen.transport.TransportProtos.EdgeNotificationMsgProto;
import org.thingsboard.server.gen.transport.TransportProtos.EntityDeleteMsg;
import org.thingsboard.server.gen.transport.TransportProtos.FromDeviceRPCResponseProto;
import org.thingsboard.server.gen.transport.TransportProtos.QueueDeleteMsg;
import org.thingsboard.server.gen.transport.TransportProtos.QueueUpdateMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ResourceDeleteMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ResourceUpdateMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCalculatedFieldNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdgeMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdgeNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToRuleEngineNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToTransportMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import org.thingsboard.server.queue.TbQueueCallback;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.common.MultipleTbQueueCallbackWrapper;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.common.TbRuleEngineProducerService;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.TopicService;
import org.thingsboard.server.queue.provider.TbQueueProducerProvider;
import org.thingsboard.server.service.gateway_device.GatewayNotificationsService;
import org.thingsboard.server.service.ota.OtaPackageStateService;
import org.thingsboard.server.service.profile.TbAssetProfileCache;
import org.thingsboard.server.service.profile.TbDeviceProfileCache;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.thingsboard.server.common.util.ProtoUtils.toProto;

/**
 * {@link TbClusterService} 的默认实现，作为集群消息分发的统一门面。
 * <p>
 * 通过 {@link PartitionService} 解析目标分区，再经 {@link TbQueueProducerProvider} 将消息投递到对应队列。
 * 涵盖向 Core、Rule Engine、Transport、Edge、Version Control、Calculated Field 等服务的推送，
 * 以及组件生命周期、队列变更的广播，和设备/租户配置变更通知。
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DefaultTbClusterService implements TbClusterService {

    /** 是否启用集群消息统计输出 */
    @Value("${cluster.stats.enabled:false}")
    private boolean statsEnabled;
    /** 是否启用 Edge 功能 */
    @Value("${edges.enabled:true}")
    protected boolean edgesEnabled;

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
    /** 发往 Edge 的业务消息计数 */
    private final AtomicInteger toEdgeMsgs = new AtomicInteger(0);
    /** 发往 Edge 的通知消息计数 */
    private final AtomicInteger toEdgeNfs = new AtomicInteger(0);

    /** 分区解析服务，用于确定消息目标 Topic 分区 */
    @Autowired
    @Lazy
    private PartitionService partitionService;

    /** 队列生产者提供者，按服务类型获取对应 Producer */
    @Autowired
    @Lazy
    private TbQueueProducerProvider producerProvider;

    /** Rule Engine 专用生产者服务，封装 TbMsg 发送逻辑 */
    @Autowired
    private TbRuleEngineProducerService ruleEngineProducerService;

    /** OTA 包状态服务（可选，单体部署时可能不存在） */
    @Autowired(required = false)
    @Lazy
    private OtaPackageStateService otaPackageStateService;

    /** Topic 名称解析服务 */
    private final TopicService topicService;
    /** 设备配置缓存，用于解析 Rule Engine 路由 */
    private final TbDeviceProfileCache deviceProfileCache;
    /** 资产配置缓存，用于解析 Rule Engine 路由 */
    private final TbAssetProfileCache assetProfileCache;
    /** 网关设备通知服务（可选） */
    private final Optional<GatewayNotificationsService> gatewayNotificationsService;
    /** Edge 数据访问服务 */
    private final EdgeService edgeService;
    /** Edge 与 Core 服务实例 ID 的映射缓存 */
    private final TbTransactionalCache<EdgeId, String> edgeIdServiceIdCache;

    /**
     * 向 Core 服务推送业务消息。
     * 路由目标：根据 tenantId + entityId 解析 TB_CORE 分区。
     *
     * @param tenantId 租户 ID
     * @param entityId 实体 ID（用于分区哈希）
     * @param msg      Core 消息体
     * @param callback 发送完成回调
     */
    @Override
    public void pushMsgToCore(TenantId tenantId, EntityId entityId, ToCoreMsg msg, TbQueueCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.TB_CORE, tenantId, entityId);
        producerProvider.getTbCoreMsgProducer().send(tpi, new TbProtoQueueMsg<>(UUID.randomUUID(), msg), callback);
        toCoreMsgs.incrementAndGet();
    }

    /**
     * 向指定 Core 分区推送业务消息（调用方已解析分区）。
     * 路由目标：传入的 TopicPartitionInfo。
     */
    @Override
    public void pushMsgToCore(TopicPartitionInfo tpi, UUID msgId, ToCoreMsg msg, TbQueueCallback callback) {
        producerProvider.getTbCoreMsgProducer().send(tpi, new TbProtoQueueMsg<>(msgId, msg), callback);
        toCoreMsgs.incrementAndGet();
    }

    /**
     * 向 Core 推送设备 Actor 通知消息。
     * 路由目标：按 deviceId 解析 TB_CORE 分区。
     *
     * @param msg      设备 Actor 通知
     * @param callback 发送完成回调
     */
    @Override
    public void pushMsgToCore(ToDeviceActorNotificationMsg msg, TbQueueCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.TB_CORE, msg.getTenantId(), msg.getDeviceId());
        log.trace("PUSHING msg: {} to:{}", msg, tpi);
        ToCoreMsg toCoreMsg = ToCoreMsg.newBuilder().setToDeviceActorNotification(toProto(msg)).build();
        producerProvider.getTbCoreMsgProducer().send(tpi, new TbProtoQueueMsg<>(msg.getDeviceId().getId(), toCoreMsg), callback);
        toCoreMsgs.incrementAndGet();
    }

    /**
     * 向所有 Core 实例广播通知消息。
     * 路由目标：每个 TB_CORE 服务实例的 notifications Topic。
     */
    @Override
    public void broadcastToCore(ToCoreNotificationMsg toCoreMsg) {
        UUID msgId = UUID.randomUUID();
        TbQueueProducer<TbProtoQueueMsg<ToCoreNotificationMsg>> toCoreNfProducer = producerProvider.getTbCoreNotificationsMsgProducer();
        Set<String> tbCoreServices = partitionService.getAllServiceIds(ServiceType.TB_CORE);
        for (String serviceId : tbCoreServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_CORE, serviceId);
            toCoreNfProducer.send(tpi, new TbProtoQueueMsg<>(msgId, toCoreMsg), null);
            toCoreNfs.incrementAndGet();
        }
    }

    /**
     * 向所有 Rule Engine 实例广播 Calculated Field 通知。
     * 路由目标：每个 TB_RULE_ENGINE 服务实例的 CF notifications Topic。
     */
    @Override
    public void broadcastToCalculatedFields(ToCalculatedFieldNotificationMsg toCfMsg, TbQueueCallback callback) {
        UUID msgId = UUID.randomUUID();
        TbQueueProducer<TbProtoQueueMsg<ToCalculatedFieldNotificationMsg>> toCfProducer = producerProvider.getCalculatedFieldsNotificationsMsgProducer();
        Set<String> tbReServices = partitionService.getAllServiceIds(ServiceType.TB_RULE_ENGINE);
        MultipleTbQueueCallbackWrapper callbackWrapper = new MultipleTbQueueCallbackWrapper(tbReServices.size(), callback);
        for (String serviceId : tbReServices) {
            TopicPartitionInfo tpi = topicService.getCalculatedFieldNotificationsTopic(serviceId);
            toCfProducer.send(tpi, new TbProtoQueueMsg<>(msgId, toCfMsg), callbackWrapper);
            toRuleEngineNfs.incrementAndGet();
        }
    }

    /**
     * 向 Version Control 服务推送消息。
     * 路由目标：TB_VC_EXECUTOR 分区（按 tenantId 哈希）。
     */
    @Override
    public void pushMsgToVersionControl(TenantId tenantId, ToVersionControlServiceMsg msg, TbQueueCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.TB_VC_EXECUTOR, TenantId.SYS_TENANT_ID, tenantId);
        log.trace("PUSHING msg: {} to:{}", msg, tpi);
        producerProvider.getTbVersionControlMsgProducer().send(tpi, new TbProtoQueueMsg<>(tenantId.getId(), msg), callback);
        //TODO: ashvayka
        toCoreMsgs.incrementAndGet();
    }

    /**
     * 向指定 Core 实例推送设备 RPC 响应通知。
     * 路由目标：指定 serviceId 的 TB_CORE notifications Topic。
     */
    @Override
    public void pushNotificationToCore(String serviceId, FromDeviceRpcResponse response, TbQueueCallback callback) {
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_CORE, serviceId);
        log.trace("PUSHING msg: {} to:{}", response, tpi);
        FromDeviceRPCResponseProto.Builder builder = FromDeviceRPCResponseProto.newBuilder()
                .setRequestIdMSB(response.getId().getMostSignificantBits())
                .setRequestIdLSB(response.getId().getLeastSignificantBits())
                .setError(response.getError().isPresent() ? response.getError().get().ordinal() : -1);
        response.getResponse().ifPresent(builder::setResponse);
        ToCoreNotificationMsg msg = ToCoreNotificationMsg.newBuilder().setFromDeviceRpcResponse(builder).build();
        producerProvider.getTbCoreNotificationsMsgProducer().send(tpi, new TbProtoQueueMsg<>(response.getId(), msg), callback);
        toCoreNfs.incrementAndGet();
    }

    /**
     * 向指定 Core 实例推送 REST API 调用响应通知。
     * 路由目标：指定 targetServiceId 的 TB_CORE notifications Topic。
     */
    @Override
    public void pushNotificationToCore(String targetServiceId, TransportProtos.RestApiCallResponseMsgProto responseMsgProto, TbQueueCallback callback) {
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_CORE, targetServiceId);
        ToCoreNotificationMsg msg = ToCoreNotificationMsg.newBuilder().setRestApiCallResponseMsg(responseMsgProto).build();
        producerProvider.getTbCoreNotificationsMsgProducer().send(tpi, new TbProtoQueueMsg<>(UUID.randomUUID(), msg), callback);
        toCoreNfs.incrementAndGet();
    }

    /**
     * 向指定 Rule Engine 分区推送业务消息（调用方已解析分区）。
     * 路由目标：传入的 TopicPartitionInfo。
     */
    @Override
    public void pushMsgToRuleEngine(TopicPartitionInfo tpi, UUID msgId, ToRuleEngineMsg msg, TbQueueCallback callback) {
        log.trace("PUSHING msg: {} to:{}", msg, tpi);
        producerProvider.getRuleEngineMsgProducer().send(tpi, new TbProtoQueueMsg<>(msgId, msg), callback);
        toRuleEngineMsgs.incrementAndGet();
    }

    /**
     * 向 Rule Engine 推送 TbMsg（使用实体 Profile 中的默认队列与规则链）。
     * 路由目标：由 ruleEngineProducerService 按 tenantId + TbMsg 解析 TB_RULE_ENGINE 分区。
     */
    @Override
    public void pushMsgToRuleEngine(TenantId tenantId, EntityId entityId, TbMsg tbMsg, TbQueueCallback callback) {
        pushMsgToRuleEngine(tenantId, entityId, tbMsg, false, callback);
    }

    /**
     * 向 Rule Engine 推送 TbMsg，可选是否沿用 TbMsg 自带的队列名。
     * 路由目标：由 ruleEngineProducerService 按 tenantId + TbMsg 解析 TB_RULE_ENGINE 分区。
     */
    @Override
    public void pushMsgToRuleEngine(TenantId tenantId, EntityId entityId, TbMsg tbMsg, boolean useQueueFromTbMsg, TbQueueCallback callback) {
        if (tenantId == null || tenantId.isNullUid()) {
            if (entityId.getEntityType().equals(EntityType.TENANT)) {
                tenantId = TenantId.fromUUID(entityId.getId());
            } else {
                log.warn("[{}][{}] Received invalid message: {}", tenantId, entityId, tbMsg);
                return;
            }
        } else {
            HasRuleEngineProfile ruleEngineProfile = getRuleEngineProfileForEntityOrElseNull(tenantId, entityId, tbMsg);
            tbMsg = transformMsg(tbMsg, ruleEngineProfile, useQueueFromTbMsg);
        }
        ruleEngineProducerService.sendToRuleEngine(producerProvider.getRuleEngineMsgProducer(), tenantId, tbMsg, callback);
        toRuleEngineMsgs.incrementAndGet();
    }

    /**
     * 根据实体类型获取 Rule Engine Profile（默认规则链与队列）。
     * 删除事件时从消息体反序列化实体以获取 Profile ID。
     */
    HasRuleEngineProfile getRuleEngineProfileForEntityOrElseNull(TenantId tenantId, EntityId entityId, TbMsg tbMsg) {
        if (entityId.getEntityType().equals(EntityType.DEVICE)) {
            if (TbMsgType.ENTITY_DELETED.equals(tbMsg.getInternalType())) {
                try {
                    Device deletedDevice = JacksonUtil.fromString(tbMsg.getData(), Device.class);
                    if (deletedDevice == null) {
                        return null;
                    }
                    return deviceProfileCache.get(tenantId, deletedDevice.getDeviceProfileId());
                } catch (Exception e) {
                    log.warn("[{}][{}] Failed to deserialize device: {}", tenantId, entityId, tbMsg, e);
                    return null;
                }
            } else {
                return deviceProfileCache.get(tenantId, new DeviceId(entityId.getId()));
            }
        } else if (entityId.getEntityType().equals(EntityType.DEVICE_PROFILE)) {
            return deviceProfileCache.get(tenantId, new DeviceProfileId(entityId.getId()));
        } else if (entityId.getEntityType().equals(EntityType.ASSET)) {
            if (TbMsgType.ENTITY_DELETED.equals(tbMsg.getInternalType())) {
                try {
                    Asset deletedAsset = JacksonUtil.fromString(tbMsg.getData(), Asset.class);
                    if (deletedAsset == null) {
                        return null;
                    }
                    return assetProfileCache.get(tenantId, deletedAsset.getAssetProfileId());
                } catch (Exception e) {
                    log.warn("[{}][{}] Failed to deserialize asset: {}", tenantId, entityId, tbMsg, e);
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
     * 按 Profile 将 TbMsg 重定向到默认规则链和/或默认队列。
     */
    private TbMsg transformMsg(TbMsg tbMsg, HasRuleEngineProfile ruleEngineProfile, boolean useQueueFromTbMsg) {
        if (ruleEngineProfile != null) {
            RuleChainId targetRuleChainId = ruleEngineProfile.getDefaultRuleChainId();
            String targetQueueName = useQueueFromTbMsg ? tbMsg.getQueueName() : ruleEngineProfile.getDefaultQueueName();

            boolean isRuleChainTransform = targetRuleChainId != null && !targetRuleChainId.equals(tbMsg.getRuleChainId());
            boolean isQueueTransform = targetQueueName != null && !targetQueueName.equals(tbMsg.getQueueName());

            if (isRuleChainTransform && isQueueTransform) {
                tbMsg = tbMsg.transform()
                        .queueName(targetQueueName)
                        .ruleChainId(targetRuleChainId)
                        .build();
            } else if (isRuleChainTransform) {
                tbMsg = tbMsg.transform()
                        .ruleChainId(targetRuleChainId)
                        .build();
            } else if (isQueueTransform) {
                tbMsg = tbMsg.transform(targetQueueName);
            }
        }
        return tbMsg;
    }

    /**
     * 向指定 Rule Engine 实例推送设备 RPC 响应通知。
     * 路由目标：指定 serviceId 的 TB_RULE_ENGINE notifications Topic。
     */
    @Override
    public void pushNotificationToRuleEngine(String serviceId, FromDeviceRpcResponse response, TbQueueCallback callback) {
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_RULE_ENGINE, serviceId);
        log.trace("PUSHING msg: {} to:{}", response, tpi);
        FromDeviceRPCResponseProto.Builder builder = FromDeviceRPCResponseProto.newBuilder()
                .setRequestIdMSB(response.getId().getMostSignificantBits())
                .setRequestIdLSB(response.getId().getLeastSignificantBits())
                .setError(response.getError().isPresent() ? response.getError().get().ordinal() : -1);
        response.getResponse().ifPresent(builder::setResponse);
        ToRuleEngineNotificationMsg msg = ToRuleEngineNotificationMsg.newBuilder().setFromDeviceRpcResponse(builder).build();
        producerProvider.getRuleEngineNotificationsMsgProducer().send(tpi, new TbProtoQueueMsg<>(response.getId(), msg), callback);
        toRuleEngineNfs.incrementAndGet();
    }

    /**
     * 向指定 Transport 实例推送通知消息。
     * 路由目标：指定 serviceId 的 TB_TRANSPORT notifications Topic。
     */
    @Override
    public void pushNotificationToTransport(String serviceId, ToTransportMsg response, TbQueueCallback callback) {
        if (serviceId == null || serviceId.isEmpty()) {
            log.trace("pushNotificationToTransport: skipping message without serviceId [{}], (ToTransportMsg) response [{}]", serviceId, response);
            if (callback != null) {
                callback.onSuccess(null); // 无有效 serviceId 时视为已发送，回调无有效载荷
            }
            return;
        }
        TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_TRANSPORT, serviceId);
        log.trace("PUSHING msg: {} to:{}", response, tpi);
        producerProvider.getTransportNotificationsMsgProducer().send(tpi, new TbProtoQueueMsg<>(UUID.randomUUID(), response), callback);
        toTransportNfs.incrementAndGet();
    }

    /**
     * 向 Calculated Field 队列推送消息。
     * 路由目标：TB_RULE_ENGINE 的 CF 专用队列分区。
     */
    @Override
    public void pushMsgToCalculatedFields(TenantId tenantId, EntityId entityId, ToCalculatedFieldMsg msg, TbQueueCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.TB_RULE_ENGINE, DataConstants.CF_QUEUE_NAME, tenantId, entityId);
        pushMsgToCalculatedFields(tpi, UUID.randomUUID(), msg, callback);
    }

    /**
     * 向指定 Calculated Field 分区推送消息（调用方已解析分区）。
     * 路由目标：传入的 TopicPartitionInfo。
     */
    @Override
    public void pushMsgToCalculatedFields(TopicPartitionInfo tpi, UUID msgId, ToCalculatedFieldMsg msg, TbQueueCallback callback) {
        log.trace("PUSHING msg: {} to:{}", msg, tpi);
        producerProvider.getCalculatedFieldsMsgProducer().send(tpi, new TbProtoQueueMsg<>(msgId, msg), callback);
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
    public void onDeviceProfileChange(DeviceProfile deviceProfile, DeviceProfile oldDeviceProfile, TbQueueCallback callback) {
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
    public void onTenantProfileChange(TenantProfile tenantProfile, TbQueueCallback callback) {
        broadcastEntityChangeToTransport(TenantId.SYS_TENANT_ID, tenantProfile.getId(), tenantProfile, callback);
    }

    /** 租户变更：广播 EntityUpdateMsg 至 Transport。 */
    @Override
    public void onTenantChange(Tenant tenant, TbQueueCallback callback) {
        broadcastEntityChangeToTransport(TenantId.SYS_TENANT_ID, tenant.getId(), tenant, callback);
    }

    /** API 用量状态变更：通知 Transport 并广播生命周期事件。 */
    @Override
    public void onApiStateChange(ApiUsageState apiUsageState, TbQueueCallback callback) {
        broadcastEntityChangeToTransport(apiUsageState.getTenantId(), apiUsageState.getId(), apiUsageState, callback);
        broadcast(new ComponentLifecycleMsg(apiUsageState.getTenantId(), apiUsageState.getId(), ComponentLifecycleEvent.UPDATED));
    }

    /** 设备配置删除：广播 EntityDeleteMsg 至 Transport。 */
    @Override
    public void onDeviceProfileDelete(DeviceProfile entity, TbQueueCallback callback) {
        broadcastEntityDeleteToTransport(entity.getTenantId(), entity.getId(), entity.getName(), callback);
    }

    /** 租户配置删除：广播 EntityDeleteMsg 至 Transport。 */
    @Override
    public void onTenantProfileDelete(TenantProfile entity, TbQueueCallback callback) {
        broadcastEntityDeleteToTransport(TenantId.SYS_TENANT_ID, entity.getId(), entity.getName(), callback);
    }

    /** 租户删除：广播 EntityDeleteMsg 至 Transport。 */
    @Override
    public void onTenantDelete(Tenant entity, TbQueueCallback callback) {
        broadcastEntityDeleteToTransport(TenantId.SYS_TENANT_ID, entity.getId(), entity.getName(), callback);
    }

    /** 设备删除：通知网关、Transport、设备状态服务，并广播生命周期 DELETED 事件。 */
    @Override
    public void onDeviceDeleted(TenantId tenantId, Device device, TbQueueCallback callback) {
        DeviceId deviceId = device.getId();
        gatewayNotificationsService.ifPresent(s -> s.onDeviceDeleted(device));
        broadcastEntityDeleteToTransport(tenantId, deviceId, device.getName(), callback);
        sendDeviceStateServiceEvent(tenantId, deviceId, false, false, true);
        broadcastEntityStateChangeEvent(tenantId, deviceId, ComponentLifecycleEvent.DELETED);
    }

    /** 资产删除：广播生命周期 DELETED 事件。 */
    @Override
    public void onAssetDeleted(TenantId tenantId, Asset asset, TbQueueCallback callback) {
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
    public void onResourceChange(TbResourceInfo resource, TbQueueCallback callback) {
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
    public void onResourceDeleted(TbResourceInfo resource, TbQueueCallback callback) {
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
    private <T> void broadcastEntityChangeToTransport(TenantId tenantId, EntityId entityid, T entity, TbQueueCallback callback) {
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
    private void broadcastEntityDeleteToTransport(TenantId tenantId, EntityId entityId, String name, TbQueueCallback callback) {
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
    private void broadcast(ToTransportMsg transportMsg, TbQueueCallback callback) {
        Set<String> tbTransportServices = partitionService.getAllServiceIds(ServiceType.TB_TRANSPORT);
        broadcast(transportMsg, tbTransportServices, callback);
    }

    /**
     * 向支持指定 Transport 类型的实例广播通知消息。
     */
    private void broadcast(ToTransportMsg transportMsg, String transportType, TbQueueCallback callback) {
        Set<String> tbTransportServices = partitionService.getAllServices(ServiceType.TB_TRANSPORT).stream()
                .filter(info -> info.getTransportsList().contains(transportType))
                .map(TransportProtos.ServiceInfo::getServiceId).collect(Collectors.toSet());
        broadcast(transportMsg, tbTransportServices, callback);
    }

    /**
     * 向给定 Transport 服务 ID 集合广播通知消息。
     *
     * @param transportMsg          Transport 消息体
     * @param tbTransportServices   目标 Transport 服务 ID 集合
     * @param callback              发送完成回调（多实例时用 MultipleTbQueueCallbackWrapper 聚合）
     */
    private void broadcast(ToTransportMsg transportMsg, Set<String> tbTransportServices, TbQueueCallback callback) {
        TbQueueProducer<TbProtoQueueMsg<ToTransportMsg>> toTransportNfProducer = producerProvider.getTransportNotificationsMsgProducer();
        TbQueueCallback proxyCallback = callback != null ? new MultipleTbQueueCallbackWrapper(tbTransportServices.size(), callback) : null;
        for (String transportServiceId : tbTransportServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_TRANSPORT, transportServiceId);
            toTransportNfProducer.send(tpi, new TbProtoQueueMsg<>(UUID.randomUUID(), transportMsg), proxyCallback);
            toTransportNfs.incrementAndGet();
        }
    }

    /**
     * 向 Edge 队列推送业务消息。
     * 路由目标：TB_CORE 的 Edge 专用队列分区。
     */
    @Override
    public void pushMsgToEdge(TenantId tenantId, EntityId entityId, ToEdgeMsg msg, TbQueueCallback callback) {
        TopicPartitionInfo tpi = partitionService.resolve(ServiceType.TB_CORE, DataConstants.EDGE_QUEUE_NAME, tenantId, entityId);
        TbQueueProducer<TbProtoQueueMsg<ToEdgeMsg>> toEdgeProducer = producerProvider.getTbEdgeMsgProducer();
        toEdgeProducer.send(tpi, new TbProtoQueueMsg<>(UUID.randomUUID(), msg), callback);
        toEdgeMsgs.incrementAndGet();
    }

    /**
     * 处理 Edge 高优先级事件，推送至 Edge notifications Topic。
     */
    @Override
    public void onEdgeHighPriorityMsg(EdgeHighPriorityMsg msg) {
        log.trace("[{}] Processing edge event for edgeId: {}", msg.getTenantId(), msg.getEdgeEvent().getEdgeId());
        ToEdgeNotificationMsg toEdgeNotificationMsg = ToEdgeNotificationMsg.newBuilder().setEdgeHighPriority(toProto(msg)).build();
        processEdgeNotification(msg.getEdgeEvent().getEdgeId(), toEdgeNotificationMsg);
    }

    /**
     * 处理 Edge 事件更新通知，推送至 Edge notifications Topic。
     */
    @Override
    public void onEdgeEventUpdate(EdgeEventUpdateMsg msg) {
        log.trace("[{}] Processing edge event update for edgeId: {}", msg.getTenantId(), msg.getEdgeId());
        ToEdgeNotificationMsg toEdgeNotificationMsg = ToEdgeNotificationMsg.newBuilder().setEdgeEventUpdate(toProto(msg)).build();
        processEdgeNotification(msg.getEdgeId(), toEdgeNotificationMsg);
    }

    /**
     * 处理 Edge 生命周期状态变更，推送至 Edge notifications Topic。
     */
    @Override
    public void onEdgeStateChangeEvent(ComponentLifecycleMsg msg) {
        log.trace("[{}] Processing {} state change event: {}", msg.getTenantId(), EntityType.EDGE, msg.getEvent());
        ComponentLifecycleMsgProto componentLifecycleMsgProto = toProto(msg);
        ToEdgeNotificationMsg toEdgeNotificationMsg = ToEdgeNotificationMsg.newBuilder().setComponentLifecycle(componentLifecycleMsgProto).build();
        processEdgeNotification((EdgeId) msg.getEntityId(), toEdgeNotificationMsg);
    }

    /**
     * 向 Edge 推送同步请求通知。
     */
    @Override
    public void pushEdgeSyncRequestToEdge(ToEdgeSyncRequest request) {
        log.trace("[{}] Processing edge sync request for edgeId: {}", request.getTenantId(), request.getEdgeId());
        ToEdgeNotificationMsg toEdgeNotificationMsg = ToEdgeNotificationMsg.newBuilder().setToEdgeSyncRequest(toProto(request)).build();
        processEdgeNotification(request.getEdgeId(), toEdgeNotificationMsg);
    }

    /**
     * 将 Edge 同步响应推送回发起请求的 Core 实例。
     */
    @Override
    public void pushEdgeSyncResponseToCore(FromEdgeSyncResponse response, String requestServiceId) {
        log.trace("[{}] Processing edge sync response for edgeId: {}", response.getTenantId(), response.getEdgeId());
        ToEdgeNotificationMsg toEdgeNotificationMsg = ToEdgeNotificationMsg.newBuilder().setFromEdgeSyncResponse(toProto(response)).build();
        pushMsgToEdgeNotification(toEdgeNotificationMsg, requestServiceId);
    }

    /**
     * 分发 Edge 通知：优先按 edgeId 缓存定位 Core 实例；未命中则广播至所有 Core。
     */
    private void processEdgeNotification(EdgeId edgeId, ToEdgeNotificationMsg toEdgeNotificationMsg) {
        if (edgesEnabled) {
            var serviceIdOpt = Optional.ofNullable(edgeIdServiceIdCache.get(edgeId));
            serviceIdOpt.ifPresentOrElse(
                    serviceId -> pushMsgToEdgeNotification(toEdgeNotificationMsg, serviceId.get()),
                    () -> broadcastEdgeNotification(edgeId, toEdgeNotificationMsg)
            );
        } else {
            log.trace("Edges disabled. Ignoring edge notification {} for edgeId: {}", toEdgeNotificationMsg, edgeId);
        }
    }

    /**
     * 向指定 Core 实例的 Edge notifications Topic 推送通知。
     */
    private void pushMsgToEdgeNotification(ToEdgeNotificationMsg toEdgeNotificationMsg, String serviceId) {
        TopicPartitionInfo tpi = topicService.getEdgeNotificationsTopic(serviceId);
        TbQueueProducer<TbProtoQueueMsg<ToEdgeNotificationMsg>> toEdgeNotificationProducer = producerProvider.getTbEdgeNotificationsMsgProducer();
        toEdgeNotificationProducer.send(tpi, new TbProtoQueueMsg<>(UUID.randomUUID(), toEdgeNotificationMsg), null);
        toEdgeNfs.incrementAndGet();
    }

    /**
     * 向所有 Core 实例广播 Edge 通知（缓存未命中时的兜底策略）。
     */
    private void broadcastEdgeNotification(EdgeId edgeId, ToEdgeNotificationMsg toEdgeNotificationMsg) {
        TbQueueProducer<TbProtoQueueMsg<ToEdgeNotificationMsg>> toEdgeNotificationProducer = producerProvider.getTbEdgeNotificationsMsgProducer();
        Set<String> serviceIds = partitionService.getAllServiceIds(ServiceType.TB_CORE);
        for (String serviceId : serviceIds) {
            TopicPartitionInfo tpi = topicService.getEdgeNotificationsTopic(serviceId);
            toEdgeNotificationProducer.send(tpi, new TbProtoQueueMsg<>(edgeId.getId(), toEdgeNotificationMsg), null);
            toEdgeNfs.incrementAndGet();
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
        TbQueueProducer<TbProtoQueueMsg<ToRuleEngineNotificationMsg>> toRuleEngineProducer = producerProvider.getRuleEngineNotificationsMsgProducer();
        Set<String> tbRuleEngineServices = partitionService.getAllServiceIds(ServiceType.TB_RULE_ENGINE);
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
            TbQueueProducer<TbProtoQueueMsg<ToCoreNotificationMsg>> toCoreNfProducer = producerProvider.getTbCoreNotificationsMsgProducer();
            Set<String> tbCoreServices = partitionService.getAllServiceIds(ServiceType.TB_CORE);
            for (String serviceId : tbCoreServices) {
                TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_CORE, serviceId);
                ToCoreNotificationMsg toCoreMsg = ToCoreNotificationMsg.newBuilder().setComponentLifecycle(componentLifecycleMsgProto).build();
                toCoreNfProducer.send(tpi, new TbProtoQueueMsg<>(msg.getEntityId().getId(), toCoreMsg), null);
                toCoreNfs.incrementAndGet();
            }
            // 单体模式下 Core 与 RE 共用 serviceId，从 RE 集合中移除以避免重复通知
            tbRuleEngineServices.removeAll(tbCoreServices);
        }
        // 广播至剩余 Rule Engine 实例
        for (String serviceId : tbRuleEngineServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_RULE_ENGINE, serviceId);
            ToRuleEngineNotificationMsg toRuleEngineMsg = ToRuleEngineNotificationMsg.newBuilder().setComponentLifecycle(componentLifecycleMsgProto).build();
            toRuleEngineProducer.send(tpi, new TbProtoQueueMsg<>(msg.getEntityId().getId(), toRuleEngineMsg), null);
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
            int toEdgeMsgCnt = toEdgeMsgs.getAndSet(0);
            int toEdgeNfsCnt = toEdgeNfs.getAndSet(0);
            if (toCoreMsgCnt > 0 || toCoreNfsCnt > 0 || toRuleEngineMsgsCnt > 0 || toRuleEngineNfsCnt > 0 || toTransportNfsCnt > 0 || toEdgeMsgCnt > 0 || toEdgeNfsCnt > 0) {
                log.info("To TbCore: [{}] messages [{}] notifications; To TbRuleEngine: [{}] messages [{}] notifications; To Transport: [{}] notifications;" +
                        "To Edge: [{}] messages [{}] notifications", toCoreMsgCnt, toCoreNfsCnt, toRuleEngineMsgsCnt, toRuleEngineNfsCnt, toTransportNfsCnt, toEdgeMsgCnt, toEdgeNfsCnt);
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
    public void onCalculatedFieldUpdated(CalculatedField calculatedField, CalculatedField oldCalculatedField, TbQueueCallback callback) {
        broadcastEntityStateChangeEvent(calculatedField.getTenantId(), calculatedField.getId(), oldCalculatedField == null ? ComponentLifecycleEvent.CREATED : ComponentLifecycleEvent.UPDATED);
    }

    /** 计算字段删除：广播生命周期 DELETED 事件。 */
    @Override
    public void onCalculatedFieldDeleted(CalculatedField calculatedField, TbQueueCallback callback) {
        broadcastEntityStateChangeEvent(calculatedField.getTenantId(), calculatedField.getId(), ComponentLifecycleEvent.DELETED);
    }

    /**
     * 向 Edge 服务发送实体变更通知消息。
     * 路由目标：TB_CORE Edge 队列；设备实体还会额外通知 Core 设备 Actor。
     */
    @Override
    public void sendNotificationMsgToEdge(TenantId tenantId, EdgeId edgeId, EntityId entityId, String body, EdgeEventType type, EdgeEventActionType action, EdgeId originatorEdgeId) {
        if (!edgesEnabled) {
            return;
        }
        if (type == null) {
            if (entityId != null) {
                type = EdgeUtils.getEdgeEventTypeByEntityType(entityId.getEntityType());
            } else {
                log.trace("[{}] entity id and type are null. Ignoring this notification", tenantId);
                return;
            }
            if (type == null) {
                log.trace("[{}] edge event type is null. Ignoring this notification [{}]", tenantId, entityId);
                return;
            }
        }
        EdgeNotificationMsgProto.Builder builder = EdgeNotificationMsgProto.newBuilder();
        builder.setTenantIdMSB(tenantId.getId().getMostSignificantBits());
        builder.setTenantIdLSB(tenantId.getId().getLeastSignificantBits());
        builder.setType(type.name());
        builder.setAction(action.name());
        if (entityId != null) {
            builder.setEntityIdMSB(entityId.getId().getMostSignificantBits());
            builder.setEntityIdLSB(entityId.getId().getLeastSignificantBits());
            builder.setEntityType(entityId.getEntityType().name());
        }
        if (edgeId != null) {
            builder.setEdgeIdMSB(edgeId.getId().getMostSignificantBits());
            builder.setEdgeIdLSB(edgeId.getId().getLeastSignificantBits());
        }
        if (body != null) {
            builder.setBody(body);
        }
        if (originatorEdgeId != null) {
            builder.setOriginatorEdgeIdMSB(originatorEdgeId.getId().getMostSignificantBits());
            builder.setOriginatorEdgeIdLSB(originatorEdgeId.getId().getLeastSignificantBits());
        }
        EdgeNotificationMsgProto msg = builder.build();
        log.trace("[{}] sending notification to edge service {}", tenantId.getId(), msg);
        pushMsgToEdge(tenantId, entityId != null ? entityId : tenantId, ToEdgeMsg.newBuilder().setEdgeNotificationMsg(msg).build(), null);

        if (entityId != null && EntityType.DEVICE.equals(entityId.getEntityType())) {
            pushDeviceUpdateMessage(tenantId, edgeId, entityId, action);
        }
    }

    /**
     * 设备与 Edge 关联关系变更时，通知 Core 设备 Actor 更新 Edge 绑定。
     */
    private void pushDeviceUpdateMessage(TenantId tenantId, EdgeId edgeId, EntityId entityId, EdgeEventActionType action) {
        log.trace("{} Going to send edge update notification for device actor, device id {}, edge id {}", tenantId, entityId, edgeId);
        switch (action) {
            case ASSIGNED_TO_EDGE -> pushMsgToCore(new DeviceEdgeUpdateMsg(tenantId, new DeviceId(entityId.getId()), edgeId), null);
            case UNASSIGNED_FROM_EDGE -> {
                EdgeId relatedEdgeId = findRelatedEdgeIdIfAny(tenantId, entityId);
                pushMsgToCore(new DeviceEdgeUpdateMsg(tenantId, new DeviceId(entityId.getId()), relatedEdgeId), null);
            }
        }
    }

    /** 查询实体仍关联的 Edge ID（取消分配时用于确定新的绑定关系）。 */
    private EdgeId findRelatedEdgeIdIfAny(TenantId tenantId, EntityId entityId) {
        PageData<EdgeId> pageData = edgeService.findRelatedEdgeIdsByEntityId(tenantId, entityId, new PageLink(1));
        return Optional.ofNullable(pageData).filter(pd -> pd.getTotalElements() > 0).map(pd -> pd.getData().get(0)).orElse(null);
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
        Set<String> tbRuleEngineServices = partitionService.getAllServiceIds(ServiceType.TB_RULE_ENGINE);
        Set<String> tbCoreServices = partitionService.getAllServiceIds(ServiceType.TB_CORE);
        Set<String> tbTransportServices = partitionService.getAllServiceIds(ServiceType.TB_TRANSPORT);
        // 单体模式下各服务共用 serviceId，去重以避免重复推送
        tbTransportServices.removeAll(tbCoreServices);
        tbCoreServices.removeAll(tbRuleEngineServices);

        for (String ruleEngineServiceId : tbRuleEngineServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_RULE_ENGINE, ruleEngineServiceId);
            producerProvider.getRuleEngineNotificationsMsgProducer().send(tpi, new TbProtoQueueMsg<>(UUID.randomUUID(), ruleEngineMsg), null);
            toRuleEngineNfs.incrementAndGet();
        }
        for (String coreServiceId : tbCoreServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_CORE, coreServiceId);
            producerProvider.getTbCoreNotificationsMsgProducer().send(tpi, new TbProtoQueueMsg<>(UUID.randomUUID(), coreMsg), null);
            toCoreNfs.incrementAndGet();
        }
        for (String transportServiceId : tbTransportServices) {
            TopicPartitionInfo tpi = topicService.getNotificationsTopic(ServiceType.TB_TRANSPORT, transportServiceId);
            producerProvider.getTransportNotificationsMsgProducer().send(tpi, new TbProtoQueueMsg<>(UUID.randomUUID(), transportMsg), null);
            toTransportNfs.incrementAndGet();
        }
    }

}
