package com.jnks.iot.server.cluster;

import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.JnksIotResourceInfo;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.asset.Asset;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.ToDeviceActorNotificationMsg;
import com.jnks.iot.server.common.msg.plugin.ComponentLifecycleMsg;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponse;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.RestApiCallResponseMsgProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueClusterService;

import java.util.UUID;

/**
 *
 */
public interface JnksIotClusterService extends JnksIotQueueClusterService {

    void pushMsgToCore(TopicPartitionInfo tpi, UUID msgKey, ToCoreMsg msg, JnksIotQueueCallback callback);

    void pushMsgToCore(TenantId tenantId, EntityId entityId, ToCoreMsg msg, JnksIotQueueCallback callback);

    void pushMsgToCore(ToDeviceActorNotificationMsg msg, JnksIotQueueCallback callback);

    void broadcastToCore(ToCoreNotificationMsg msg);

    void broadcastToCalculatedFields(ToCalculatedFieldNotificationMsg build, JnksIotQueueCallback callback);

    void pushMsgToVersionControl(TenantId tenantId, ToVersionControlServiceMsg msg, JnksIotQueueCallback callback);

    void pushNotificationToCore(String targetServiceId, FromDeviceRpcResponse response, JnksIotQueueCallback callback);

    void pushNotificationToCore(String targetServiceId, RestApiCallResponseMsgProto msg, JnksIotQueueCallback callback);

    void pushMsgToRuleEngine(TopicPartitionInfo tpi, UUID msgId, ToRuleEngineMsg msg, JnksIotQueueCallback callback);

    void pushMsgToRuleEngine(TenantId tenantId, EntityId entityId, JnksIotMsg msg, JnksIotQueueCallback callback);

    void pushMsgToRuleEngine(TenantId tenantId, EntityId entityId, JnksIotMsg msg, boolean useQueueFromJnksIotMsg, JnksIotQueueCallback callback);

    void pushNotificationToRuleEngine(String targetServiceId, FromDeviceRpcResponse response, JnksIotQueueCallback callback);

    void pushNotificationToTransport(String targetServiceId, ToTransportMsg response, JnksIotQueueCallback callback);

    void pushMsgToCalculatedFields(TenantId tenantId, EntityId entityId, TransportProtos.ToCalculatedFieldMsg msg, JnksIotQueueCallback callback);

    void pushMsgToCalculatedFields(TopicPartitionInfo tpi, UUID msgId, ToCalculatedFieldMsg msg, JnksIotQueueCallback callback);


    /**
     * 广播实体状态改变事件
     * @param tenantId 租户id
     * @param entityId 实体id
     * @param state 状态
     */
    void broadcastEntityStateChangeEvent(TenantId tenantId, EntityId entityId, ComponentLifecycleEvent state);

    void onDeviceProfileChange(DeviceProfile deviceProfile, DeviceProfile oldDeviceProfile, JnksIotQueueCallback callback);

    void onDeviceProfileDelete(DeviceProfile deviceProfile, JnksIotQueueCallback callback);

    void onTenantProfileChange(TenantProfile tenantProfile, JnksIotQueueCallback callback);

    void onTenantProfileDelete(TenantProfile tenantProfile, JnksIotQueueCallback callback);

    void onTenantChange(Tenant tenant, JnksIotQueueCallback callback);

    void onTenantDelete(Tenant tenant, JnksIotQueueCallback callback);

    void onApiStateChange(ApiUsageState apiUsageState, JnksIotQueueCallback callback);

    /**
     * 处理设备更新
     * @param device 新设备对象
     * @param old 旧设备对象
     */
    void onDeviceUpdated(Device device, Device old);

    void onDeviceDeleted(TenantId tenantId, Device device, JnksIotQueueCallback callback);

    void onDeviceAssignedToTenant(TenantId oldTenantId, Device device);

    void onAssetUpdated(Asset asset, Asset old);

    void onAssetDeleted(TenantId tenantId, Asset asset, JnksIotQueueCallback callback);

    void onResourceChange(JnksIotResourceInfo resource, JnksIotQueueCallback callback);

    void onResourceDeleted(JnksIotResourceInfo resource, JnksIotQueueCallback callback);

    void onCalculatedFieldUpdated(CalculatedField calculatedField, CalculatedField oldCalculatedField, JnksIotQueueCallback callback);

    void onCalculatedFieldDeleted(CalculatedField calculatedField, JnksIotQueueCallback callback);

}
