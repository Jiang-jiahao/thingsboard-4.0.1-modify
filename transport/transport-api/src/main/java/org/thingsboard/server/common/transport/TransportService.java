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
package org.thingsboard.server.common.transport;

import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.data.rpc.RpcStatus;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.transport.auth.GetOrCreateDeviceFromGatewayResponse;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.common.transport.service.SessionMetaData;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.ClaimDeviceMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetAttributeRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetDeviceCredentialsRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetDeviceCredentialsResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetDeviceRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetDeviceResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetEntityProfileRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetEntityProfileResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetOrCreateDeviceFromGatewayRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetOtaPackageRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetOtaPackageResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetResourceRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetResourceResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetSnmpDevicesRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetSnmpDevicesResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetTcpDevicesRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetTcpDevicesResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.LwM2MRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.LwM2MResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.PostAttributeMsg;
import org.thingsboard.server.gen.transport.TransportProtos.PostTelemetryMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ProvisionDeviceRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ProvisionDeviceResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.SessionEventMsg;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.gen.transport.TransportProtos.SubscribeToAttributeUpdatesMsg;
import org.thingsboard.server.gen.transport.TransportProtos.SubscribeToRPCMsg;
import org.thingsboard.server.gen.transport.TransportProtos.SubscriptionInfoProto;
import org.thingsboard.server.gen.transport.TransportProtos.ToDeviceRpcRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToDeviceRpcResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToServerRpcRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.TransportToDeviceActorMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ValidateBasicMqttCredRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ValidateDeviceLwM2MCredentialsRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ValidateDeviceTokenRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ValidateDeviceX509CertRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ValidateOrCreateDeviceX509CertRequestMsg;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashvayka on 04.10.18.
 */
public interface TransportService {

    /**
     * 获取实体配置信息（如设备配置、租户配置等）。
     * 这是一个同步请求，直接返回响应。
     *
     * @param msg 包含实体类型和 ID 的请求消息
     * @return 实体配置响应消息
     */
    GetEntityProfileResponseMsg getEntityProfile(GetEntityProfileRequestMsg msg);

    /**
     * 获取所有队列的路由信息。
     * 用于获取消息队列的分区分配情况，通常在集群启动或配置变更时调用。
     *
     * @param msg 请求消息
     * @return 队列路由信息列表
     */
    List<TransportProtos.GetQueueRoutingInfoResponseMsg> getQueueRoutingInfo(TransportProtos.GetAllQueueRoutingInfoRequestMsg msg);

    /**
     * 获取资源（如 LwM2M 模型、OTA 固件包等）。
     * 这是一个同步请求。
     *
     * @param msg 资源请求消息
     * @return 资源响应消息
     */
    GetResourceResponseMsg getResource(GetResourceRequestMsg msg);

    /**
     * 获取所有 SNMP 设备的 ID 列表。
     * 用于 SNMP 协议适配器获取需要轮询的设备。
     *
     * @param requestMsg SNMP 设备请求消息
     * @return SNMP 设备响应消息，包含设备 ID 列表
     */
    GetSnmpDevicesResponseMsg getSnmpDevicesIds(GetSnmpDevicesRequestMsg requestMsg);

    /**
     * 获取使用 TCP 传输类型的设备 ID 列表（用于 CLIENT 模式主动建连等）。
     */
    GetTcpDevicesResponseMsg getTcpDevicesIds(GetTcpDevicesRequestMsg requestMsg);

    /**
     * 获取设备信息。
     * 这是一个同步请求。
     *
     * @param requestMsg 设备请求消息
     * @return 设备响应消息，如果设备不存在则返回 null
     */
    GetDeviceResponseMsg getDevice(GetDeviceRequestMsg requestMsg);

    /**
     * 获取设备凭据信息。
     * 这是一个同步请求。
     *
     * @param requestMsg 设备凭据请求消息
     * @return 设备凭据响应消息
     */
    GetDeviceCredentialsResponseMsg getDeviceCredentials(GetDeviceCredentialsRequestMsg requestMsg);

    // ==================== 异步认证方法 ====================

    /**
     * 处理基于令牌的设备认证。
     *
     * @param transportType 传输类型（用于校验设备配置是否匹配）
     * @param msg           令牌认证请求
     * @param callback      异步回调，返回认证结果或错误
     */
    void process(DeviceTransportType transportType, ValidateDeviceTokenRequestMsg msg,
                 TransportServiceCallback<ValidateDeviceCredentialsResponse> callback);

    /**
     * 处理基于 MQTT 基本凭据（用户名/密码）的设备认证。
     *
     * @param transportType 传输类型
     * @param msg           基本 MQTT 凭据请求
     * @param callback      回调
     */
    void process(DeviceTransportType transportType, ValidateBasicMqttCredRequestMsg msg,
                 TransportServiceCallback<ValidateDeviceCredentialsResponse> callback);

    /**
     * 处理基于 X.509 证书的设备认证（仅验证，不创建）。
     *
     * @param transportType 传输类型
     * @param msg           X.509 证书验证请求
     * @param callback      回调
     */
    void process(DeviceTransportType transportType, ValidateDeviceX509CertRequestMsg msg,
                 TransportServiceCallback<ValidateDeviceCredentialsResponse> callback);

    /**
     * 处理基于 X.509 证书的设备认证，如果设备不存在则自动创建（用于自动注册）。
     *
     * @param transportType 传输类型
     * @param msg           验证或创建设备的 X.509 请求
     * @param callback      回调
     */
    void process(DeviceTransportType transportType, ValidateOrCreateDeviceX509CertRequestMsg msg,
                 TransportServiceCallback<ValidateDeviceCredentialsResponse> callback);

    /**
     * 处理 LwM2M 设备凭据认证。
     *
     * @param msg      LwM2M 凭据请求
     * @param callback 回调
     */
    void process(ValidateDeviceLwM2MCredentialsRequestMsg msg,
                 TransportServiceCallback<ValidateDeviceCredentialsResponse> callback);

    /**
     * 处理从网关侧获取或创建设备的请求（用于网关子设备自动注册）。
     *
     * @param tenantId 租户 ID
     * @param msg      请求消息，包含网关 ID 和设备名称等信息
     * @param callback 回调，返回设备信息或错误
     */
    void process(TenantId tenantId, GetOrCreateDeviceFromGatewayRequestMsg msg,
                 TransportServiceCallback<GetOrCreateDeviceFromGatewayResponse> callback);

    /**
     * 处理设备预配置请求（Provisioning）。
     *
     * @param msg      配置请求消息
     * @param callback 回调，返回配置响应
     */
    void process(ProvisionDeviceRequestMsg msg,
                 TransportServiceCallback<ProvisionDeviceResponseMsg> callback);

    // ==================== 配置更新通知 ====================

    /**
     * 当设备配置（Device Profile）更新时调用，通知所有相关会话更新配置信息。
     *
     * @param deviceProfile 更新后的设备配置
     */
    void onProfileUpdate(DeviceProfile deviceProfile);

    // ==================== LwM2M 特定方法 ====================
    /**
     * 处理 LwM2M 协议请求（如读、写、执行等）。
     *
     * @param msg      LwM2M 请求
     * @param callback 回调，返回 LwM2M 响应
     */
    void process(LwM2MRequestMsg msg,
                 TransportServiceCallback<LwM2MResponseMsg> callback);

    // ==================== 会话消息处理 ====================

    /**
     * 处理会话事件（如打开、关闭）。
     *
     * @param sessionInfo 会话信息
     * @param msg         会话事件消息
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, SessionEventMsg msg, TransportServiceCallback<Void> callback);

    /**
     * 处理设备遥测数据上报。（往规则引擎发送）
     *
     * @param sessionInfo 会话信息
     * @param msg         遥测消息
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, PostTelemetryMsg msg, TransportServiceCallback<Void> callback);

    /**
     * 处理带元数据的设备遥测数据上报。（往规则引擎发送）
     *
     * @param sessionInfo 会话信息
     * @param msg         遥测消息
     * @param md          元数据（如客户密钥、额外信息）
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, PostTelemetryMsg msg, TbMsgMetaData md, TransportServiceCallback<Void> callback);

    /**
     * 处理设备属性上报。（往规则引擎发送）
     *
     * @param sessionInfo 会话信息
     * @param msg         属性消息
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, PostAttributeMsg msg, TransportServiceCallback<Void> callback);

    /**
     * 处理带元数据的设备属性上报。（往规则引擎发送）
     *
     * @param sessionInfo 会话信息
     * @param msg         属性消息
     * @param md          元数据
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, PostAttributeMsg msg, TbMsgMetaData md, TransportServiceCallback<Void> callback);

    /**
     * 处理设备属性获取请求。
     *
     * @param sessionInfo 会话信息
     * @param msg         属性获取请求
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, GetAttributeRequestMsg msg, TransportServiceCallback<Void> callback);

    /**
     * 处理属性订阅/取消订阅。
     *
     * @param sessionInfo 会话信息
     * @param msg         订阅消息（包含 unsubscribe 标志）
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, SubscribeToAttributeUpdatesMsg msg, TransportServiceCallback<Void> callback);

    /**
     * 处理 RPC 订阅/取消订阅。
     *
     * @param sessionInfo 会话信息
     * @param msg         RPC 订阅消息
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, SubscribeToRPCMsg msg, TransportServiceCallback<Void> callback);

    /**
     * 处理从设备发往服务器的 RPC 响应（设备响应服务器发起的 RPC）。
     * 这个发起者可能是规则引擎也可能是core，但是都统一往core回复。core会判断是否是自己发起，如果不是会转发给规则引擎
     *
     * @param sessionInfo 会话信息
     * @param msg         RPC 响应
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, ToDeviceRpcResponseMsg msg, TransportServiceCallback<Void> callback);

    /**
     * 处理从设备发往服务器的 RPC 请求（设备调用服务器端方法）。（往规则引擎发送）
     *
     * @param sessionInfo 会话信息
     * @param msg         RPC 请求
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, ToServerRpcRequestMsg msg, TransportServiceCallback<Void> callback);

    /**
     * 处理发往设备的 RPC 请求（服务器调用设备），可选择是否记录活动。
     *
     * @param sessionInfo    会话信息
     * @param msg            RPC 请求
     * @param rpcStatus      RPC 状态（如 QUEUED、SENT 等）
     * @param reportActivity 是否记录设备活动（心跳）
     * @param callback       回调
     */
    void process(SessionInfoProto sessionInfo, ToDeviceRpcRequestMsg msg, RpcStatus rpcStatus, boolean reportActivity, TransportServiceCallback<Void> callback);

    /**
     * 处理发往设备的 RPC 请求（默认记录活动）。
     *
     * @param sessionInfo 会话信息
     * @param msg         RPC 请求
     * @param rpcStatus   状态
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, ToDeviceRpcRequestMsg msg, RpcStatus rpcStatus, TransportServiceCallback<Void> callback);

    /**
     * 处理订阅信息更新（通常用于网关会话管理）。
     *
     * @param sessionInfo 会话信息
     * @param msg         订阅信息
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, SubscriptionInfoProto msg, TransportServiceCallback<Void> callback);

    /**
     * 处理设备认领请求。
     *
     * @param sessionInfo 会话信息
     * @param msg         认领消息
     * @param callback    回调
     */
    void process(SessionInfoProto sessionInfo, ClaimDeviceMsg msg, TransportServiceCallback<Void> callback);

    /**
     * 处理发往设备 Actor 的通用消息（内部转发）。
     *
     * @param msg      发往设备 Actor 的消息
     * @param callback 回调
     */
    void process(TransportToDeviceActorMsg msg, TransportServiceCallback<Void> callback);

    /**
     * 处理 OTA 固件包请求。
     *
     * @param sessionInfoProto 会话信息
     * @param msg              OTA 请求
     * @param callback         回调，返回 OTA 包响应
     */
    void process(SessionInfoProto sessionInfoProto, GetOtaPackageRequestMsg msg, TransportServiceCallback<GetOtaPackageResponseMsg> callback);

    SessionMetaData registerAsyncSession(SessionInfoProto sessionInfo, SessionMsgListener listener);

    SessionMetaData registerSyncSession(SessionInfoProto sessionInfo, SessionMsgListener listener, long timeout);

    void recordActivity(SessionInfoProto sessionInfo);

    void lifecycleEvent(TenantId tenantId, DeviceId deviceId, ComponentLifecycleEvent eventType, boolean success, Throwable error);

    void errorEvent(TenantId tenantId, DeviceId deviceId, String method, Throwable error);

    void deregisterSession(SessionInfoProto sessionInfo);

    void log(SessionInfoProto sessionInfo, String msg);

    void notifyAboutUplink(SessionInfoProto sessionInfo, TransportProtos.UplinkNotificationMsg build, TransportServiceCallback<Void> empty);

    ExecutorService getCallbackExecutor();

    boolean hasSession(SessionInfoProto sessionInfo);

    void createGaugeStats(String openConnections, AtomicInteger connectionsCounter);
}
