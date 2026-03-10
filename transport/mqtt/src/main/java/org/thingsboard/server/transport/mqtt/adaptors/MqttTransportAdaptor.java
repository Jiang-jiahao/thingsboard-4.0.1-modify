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
package org.thingsboard.server.transport.mqtt.adaptors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import org.thingsboard.server.common.adaptor.AdaptorException;
import org.thingsboard.server.common.data.ota.OtaPackageType;
import org.thingsboard.server.gen.transport.TransportProtos.AttributeUpdateNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ClaimDeviceMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetAttributeRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetAttributeResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.PostAttributeMsg;
import org.thingsboard.server.gen.transport.TransportProtos.PostTelemetryMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ProvisionDeviceRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ProvisionDeviceResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToDeviceRpcRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToDeviceRpcResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToServerRpcRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToServerRpcResponseMsg;
import org.thingsboard.server.transport.mqtt.session.MqttDeviceAwareSessionContext;

import java.util.Optional;

/**
 * 将 MQTT 消息转换为内部传输层（TB自定义的消息对象） Protobuf 消息，以及将内部 Protobuf 消息转换为 MQTT 消息的方法。
 * @author Andrew Shvayka
 */
public interface MqttTransportAdaptor {

    /**
     * 全局共享的 ByteBuf 分配器，用于创建 Netty 缓冲区
     */
    ByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);


    /**
     * 将收到的 MQTT PUBLISH 消息转换为 PostTelemetryMsg（遥测数据）
     *
     * @param ctx      会话上下文
     * @param inbound  收到的 MQTT 发布消息
     * @return 转换后的遥测 Protobuf 消息
     * @throws AdaptorException 转换异常
     */
    PostTelemetryMsg convertToPostTelemetry(MqttDeviceAwareSessionContext ctx, MqttPublishMessage inbound) throws AdaptorException;

    /**
     * 将收到的 MQTT PUBLISH 消息转换为 PostAttributeMsg（属性数据）
     *
     * @param ctx      会话上下文
     * @param inbound  收到的 MQTT 发布消息
     * @return 转换后的属性 Protobuf 消息
     * @throws AdaptorException 转换异常
     */
    PostAttributeMsg convertToPostAttributes(MqttDeviceAwareSessionContext ctx, MqttPublishMessage inbound) throws AdaptorException;


    /**
     * 将收到的 MQTT PUBLISH 消息转换为 GetAttributeRequestMsg（获取属性请求）
     *
     * @param ctx       会话上下文
     * @param inbound   收到的 MQTT 发布消息
     * @param topicBase 主题基础部分（用于提取请求 ID）
     * @return 转换后的获取属性请求 Protobuf 消息
     * @throws AdaptorException 转换异常
     */
    GetAttributeRequestMsg convertToGetAttributes(MqttDeviceAwareSessionContext ctx, MqttPublishMessage inbound, String topicBase) throws AdaptorException;

    ToDeviceRpcResponseMsg convertToDeviceRpcResponse(MqttDeviceAwareSessionContext ctx, MqttPublishMessage mqttMsg, String topicBase) throws AdaptorException;

    ToServerRpcRequestMsg convertToServerRpcRequest(MqttDeviceAwareSessionContext ctx, MqttPublishMessage mqttMsg, String topicBase) throws AdaptorException;

    ClaimDeviceMsg convertToClaimDevice(MqttDeviceAwareSessionContext ctx, MqttPublishMessage inbound) throws AdaptorException;

    /**
     * 将 GetAttributeResponseMsg（获取属性响应）转换为可发送的 MQTT PUBLISH 消息（直连设备）
     *
     * @param ctx          会话上下文
     * @param responseMsg  属性响应 Protobuf 消息
     * @param topicBase    主题基础部分
     * @return 包含 MQTT 消息的 Optional（如果请求 ID 无效则返回空）
     * @throws AdaptorException 转换异常（如响应中包含错误）
     */
    Optional<MqttMessage> convertToPublish(MqttDeviceAwareSessionContext ctx, GetAttributeResponseMsg responseMsg, String topicBase) throws AdaptorException;


    /**
     * 将 GetAttributeResponseMsg 转换为可发送的 MQTT PUBLISH 消息（网关场景，携带设备名称）
     *
     * @param ctx          会话上下文
     * @param deviceName   目标子设备名称
     * @param responseMsg  属性响应 Protobuf 消息
     * @return 包含 MQTT 消息的 Optional
     * @throws AdaptorException 转换异常
     */
    Optional<MqttMessage> convertToGatewayPublish(MqttDeviceAwareSessionContext ctx, String deviceName, GetAttributeResponseMsg responseMsg) throws AdaptorException;


    /**
     * 将 AttributeUpdateNotificationMsg（属性更新通知）转换为可发送的 MQTT PUBLISH 消息（直连设备）
     *
     * @param ctx             会话上下文
     * @param notificationMsg 属性更新通知 Protobuf 消息
     * @param topic           目标主题
     * @return 包含 MQTT 消息的 Optional
     * @throws AdaptorException 转换异常
     */
    Optional<MqttMessage> convertToPublish(MqttDeviceAwareSessionContext ctx, AttributeUpdateNotificationMsg notificationMsg, String topic) throws AdaptorException;

    Optional<MqttMessage> convertToGatewayPublish(MqttDeviceAwareSessionContext ctx, String deviceName, AttributeUpdateNotificationMsg notificationMsg) throws AdaptorException;

    Optional<MqttMessage> convertToPublish(MqttDeviceAwareSessionContext ctx, ToDeviceRpcRequestMsg rpcRequest, String topicBase) throws AdaptorException;

    Optional<MqttMessage> convertToGatewayPublish(MqttDeviceAwareSessionContext ctx, String deviceName, ToDeviceRpcRequestMsg rpcRequest) throws AdaptorException;

    Optional<MqttMessage> convertToPublish(MqttDeviceAwareSessionContext ctx, ToServerRpcResponseMsg rpcResponse, String topicBase) throws AdaptorException;

    ProvisionDeviceRequestMsg convertToProvisionRequestMsg(MqttDeviceAwareSessionContext ctx, MqttPublishMessage inbound) throws AdaptorException;

    Optional<MqttMessage> convertToPublish(MqttDeviceAwareSessionContext ctx, ProvisionDeviceResponseMsg provisionResponse) throws AdaptorException;

    Optional<MqttMessage> convertToPublish(MqttDeviceAwareSessionContext ctx, byte[] firmwareChunk, String requestId, int chunk, OtaPackageType firmwareType) throws AdaptorException;

    Optional<MqttMessage> convertToGatewayDeviceDisconnectPublish(MqttDeviceAwareSessionContext ctx, String deviceName, int reasonCode) throws AdaptorException;

    /**
     * 默认方法：创建 MQTT PUBLISH 消息的便捷工具
     *
     * @param ctx              会话上下文
     * @param topic            主题
     * @param payloadInBytes   负载字节数组
     * @return 构造好的 MqttPublishMessage 对象
     */
    default MqttPublishMessage createMqttPublishMsg(MqttDeviceAwareSessionContext ctx, String topic, byte[] payloadInBytes) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, ctx.getQoSForTopic(topic), false, 0);
        MqttPublishVariableHeader header = new MqttPublishVariableHeader(topic, ctx.nextMsgId());
        ByteBuf payload = ALLOCATOR.buffer();
        payload.writeBytes(payloadInBytes);
        return new MqttPublishMessage(mqttFixedHeader, header, payload);
    }
}
