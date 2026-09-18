package org.thingsboard.server.cluster;

import org.thingsboard.server.common.data.ApiUsageState;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.TbResourceInfo;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.cf.CalculatedField;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.ToDeviceActorNotificationMsg;
import org.thingsboard.server.common.msg.plugin.ComponentLifecycleMsg;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.common.msg.rpc.FromDeviceRpcResponse;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.RestApiCallResponseMsgProto;
import org.thingsboard.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCalculatedFieldNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToTransportMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import org.thingsboard.server.queue.TbQueueCallback;
import org.thingsboard.server.queue.TbQueueClusterService;

import java.util.UUID;

/**
 *
 */
public interface TbClusterService extends TbQueueClusterService {

    void pushMsgToCore(TopicPartitionInfo tpi, UUID msgKey, ToCoreMsg msg, TbQueueCallback callback);

    void pushMsgToCore(TenantId tenantId, EntityId entityId, ToCoreMsg msg, TbQueueCallback callback);

    void pushMsgToCore(ToDeviceActorNotificationMsg msg, TbQueueCallback callback);

    void broadcastToCore(ToCoreNotificationMsg msg);

    void broadcastToCalculatedFields(ToCalculatedFieldNotificationMsg build, TbQueueCallback callback);

    void pushMsgToVersionControl(TenantId tenantId, ToVersionControlServiceMsg msg, TbQueueCallback callback);

    void pushNotificationToCore(String targetServiceId, FromDeviceRpcResponse response, TbQueueCallback callback);

    void pushNotificationToCore(String targetServiceId, RestApiCallResponseMsgProto msg, TbQueueCallback callback);

    void pushMsgToRuleEngine(TopicPartitionInfo tpi, UUID msgId, ToRuleEngineMsg msg, TbQueueCallback callback);

    void pushMsgToRuleEngine(TenantId tenantId, EntityId entityId, TbMsg msg, TbQueueCallback callback);

    void pushMsgToRuleEngine(TenantId tenantId, EntityId entityId, TbMsg msg, boolean useQueueFromTbMsg, TbQueueCallback callback);

    void pushNotificationToRuleEngine(String targetServiceId, FromDeviceRpcResponse response, TbQueueCallback callback);

    void pushNotificationToTransport(String targetServiceId, ToTransportMsg response, TbQueueCallback callback);

    void pushMsgToCalculatedFields(TenantId tenantId, EntityId entityId, TransportProtos.ToCalculatedFieldMsg msg, TbQueueCallback callback);

    void pushMsgToCalculatedFields(TopicPartitionInfo tpi, UUID msgId, ToCalculatedFieldMsg msg, TbQueueCallback callback);


    /**
     * 广播实体状态改变事件
     * @param tenantId 租户id
     * @param entityId 实体id
     * @param state 状态
     */
    void broadcastEntityStateChangeEvent(TenantId tenantId, EntityId entityId, ComponentLifecycleEvent state);

    void onDeviceProfileChange(DeviceProfile deviceProfile, DeviceProfile oldDeviceProfile, TbQueueCallback callback);

    void onDeviceProfileDelete(DeviceProfile deviceProfile, TbQueueCallback callback);

    void onTenantProfileChange(TenantProfile tenantProfile, TbQueueCallback callback);

    void onTenantProfileDelete(TenantProfile tenantProfile, TbQueueCallback callback);

    void onTenantChange(Tenant tenant, TbQueueCallback callback);

    void onTenantDelete(Tenant tenant, TbQueueCallback callback);

    void onApiStateChange(ApiUsageState apiUsageState, TbQueueCallback callback);

    /**
     * 处理设备更新
     * @param device 新设备对象
     * @param old 旧设备对象
     */
    void onDeviceUpdated(Device device, Device old);

    void onDeviceDeleted(TenantId tenantId, Device device, TbQueueCallback callback);

    void onDeviceAssignedToTenant(TenantId oldTenantId, Device device);

    void onAssetUpdated(Asset asset, Asset old);

    void onAssetDeleted(TenantId tenantId, Asset asset, TbQueueCallback callback);

    void onResourceChange(TbResourceInfo resource, TbQueueCallback callback);

    void onResourceDeleted(TbResourceInfo resource, TbQueueCallback callback);

    void onCalculatedFieldUpdated(CalculatedField calculatedField, CalculatedField oldCalculatedField, TbQueueCallback callback);

    void onCalculatedFieldDeleted(CalculatedField calculatedField, TbQueueCallback callback);

}
