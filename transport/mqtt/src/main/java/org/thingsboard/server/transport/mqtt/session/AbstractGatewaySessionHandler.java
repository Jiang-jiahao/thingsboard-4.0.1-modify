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
package org.thingsboard.server.transport.mqtt.session;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttVersion;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.server.common.adaptor.AdaptorException;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.adaptor.ProtoConverter;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.util.TbPair;
import org.thingsboard.server.common.msg.gateway.metrics.GatewayMetadata;
import org.thingsboard.server.common.msg.tools.TbRateLimitsException;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.auth.GetOrCreateDeviceFromGatewayResponse;
import org.thingsboard.server.common.transport.auth.TransportDeviceInfo;
import org.thingsboard.server.gen.transport.TransportApiProtos;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.GetOrCreateDeviceFromGatewayRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.transport.mqtt.MqttTransportContext;
import org.thingsboard.server.transport.mqtt.MqttTransportHandler;
import org.thingsboard.server.transport.mqtt.adaptors.JsonMqttAdaptor;
import org.thingsboard.server.transport.mqtt.adaptors.MqttTransportAdaptor;
import org.thingsboard.server.transport.mqtt.adaptors.ProtoMqttAdaptor;
import org.thingsboard.server.transport.mqtt.gateway.GatewayMetricsService;
import org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugConnectionState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.springframework.util.ConcurrentReferenceHashMap.ReferenceType;
import static org.thingsboard.server.common.data.DataConstants.DEFAULT_DEVICE_TYPE;
import static org.thingsboard.server.common.transport.service.DefaultTransportService.SESSION_EVENT_MSG_CLOSED;
import static org.thingsboard.server.common.transport.service.DefaultTransportService.SESSION_EVENT_MSG_OPEN;
import static org.thingsboard.server.common.transport.service.DefaultTransportService.SUBSCRIBE_TO_ATTRIBUTE_UPDATES_ASYNC_MSG;
import static org.thingsboard.server.common.transport.service.DefaultTransportService.SUBSCRIBE_TO_RPC_ASYNC_MSG;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugConnectionState.OFFLINE;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMessageType.STATE;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMessageType.messageName;

/**
 * 抽象网关会话处理器，为MQTT网关管理子设备提供基础框架。
 * 处理子设备的连接、断开、遥测、属性、RPC等操作。
 * 支持JSON和Protobuf两种负载格式。
 * Created by ashvayka on 19.01.17.
 */
@Slf4j
public abstract class AbstractGatewaySessionHandler<T extends AbstractGatewayDeviceSessionContext> {

    // 常量定义，用于日志和错误消息
    private static final String CAN_T_PARSE_VALUE = "Can't parse value: ";
    private static final String DEVICE_PROPERTY = "device";
    public static final String TELEMETRY = "telemetry";
    public static final String CLAIMING = "claiming";
    public static final String ATTRIBUTE = "attribute";
    public static final String RPC_RESPONSE = "Rpc response";
    public static final String ATTRIBUTES_REQUEST = "attributes request";

    /**
     * MQTT传输上下文，包含配置和共享资源
     */
    protected final MqttTransportContext context;

    /**
     * 传输服务，用于与后端通信
     */
    protected final TransportService transportService;

    /**
     * 网关设备的信息
     */
    protected final TransportDeviceInfo gateway;

    /**
     * 当前网关会话的唯一ID
     */
    @Getter
    protected final UUID sessionId;

    /**
     * 用于防止并发创建设备会话的锁映射，使用弱引用避免内存泄漏
     */
    private final ConcurrentMap<String, Lock> deviceCreationLockMap;

    /**
     * 已连接的子设备会话上下文映射，键为设备名
     */
    private final ConcurrentMap<String, T> devices;

    /**
     * 正在创建中的设备会话Future映射，用于异步等待设备创建完成
     */
    private final ConcurrentMap<String, ListenableFuture<T>> deviceFutures;

    /**
     * 订阅主题与QoS的映射
     */
    protected final ConcurrentMap<MqttTopicMatcher, Integer> mqttQoSMap;

    /**
     *  Netty通道上下文，用于发送消息
     */
    @Getter
    protected final ChannelHandlerContext channel;

    /**
     * 网关自身的会话上下文
     */
    protected final DeviceSessionCtx deviceSessionCtx;

    /**
     * 网关指标服务，用于收集网关和子设备指标
     */
    protected final GatewayMetricsService gatewayMetricsService;

    /**
     * 是否覆盖子设备的活动时间（通过网关心跳）
     */
    @Getter
    @Setter
    private boolean overwriteDevicesActivity = false;

    public AbstractGatewaySessionHandler(DeviceSessionCtx deviceSessionCtx, UUID sessionId, boolean overwriteDevicesActivity) {
        log.debug("[{}] Gateway connect [{}] session [{}]", deviceSessionCtx.getTenantId(), deviceSessionCtx.getDeviceId(), sessionId);
        this.context = deviceSessionCtx.getContext();
        this.transportService = context.getTransportService();
        this.deviceSessionCtx = deviceSessionCtx;
        this.gateway = deviceSessionCtx.getDeviceInfo();
        this.sessionId = sessionId;
        this.devices = new ConcurrentHashMap<>();
        this.deviceFutures = new ConcurrentHashMap<>();
        this.deviceCreationLockMap = createWeakMap();
        this.mqttQoSMap = deviceSessionCtx.getMqttQoSMap();
        this.channel = deviceSessionCtx.getChannel();
        this.overwriteDevicesActivity = overwriteDevicesActivity;
        this.gatewayMetricsService = deviceSessionCtx.getContext().getGatewayMetricsService();
    }

    /**
     * 创建弱引用映射，用于存储锁对象，防止内存泄漏
     * @return ConcurrentReferenceHashMap
     */
    ConcurrentReferenceHashMap<String, Lock> createWeakMap() {
        return new ConcurrentReferenceHashMap<>(16, ReferenceType.WEAK);
    }

    /**
     * 处理子设备断开连接的消息。
     * 根据网关的负载类型（JSON或Protobuf）调用对应的处理方法。
     */
    public void onDeviceDisconnect(MqttPublishMessage mqttMsg) throws AdaptorException {
        if (isJsonPayloadType()) {
            onDeviceDisconnectJson(mqttMsg);
        } else {
            onGatewayDeviceDisconnectProto(mqttMsg);
        }
    }

    /**
     * 处理子设备认领（Claiming）的消息。
     */
    public void onDeviceClaim(MqttPublishMessage mqttMsg) throws AdaptorException {
        int msgId = getMsgId(mqttMsg);
        ByteBuf payload = mqttMsg.payload();
        if (isJsonPayloadType()) {
            onDeviceClaimJson(msgId, payload);
        } else {
            onDeviceClaimProto(msgId, payload);
        }
    }

    /**
     * 处理子设备属性上报的消息。
     */
    public void onDeviceAttributes(MqttPublishMessage mqttMsg) throws AdaptorException {
        int msgId = getMsgId(mqttMsg);
        ByteBuf payload = mqttMsg.payload();
        if (isJsonPayloadType()) {
            onDeviceAttributesJson(msgId, payload);
        } else {
            onDeviceAttributesProto(msgId, payload);
        }
    }

    /**
     * 处理子设备属性请求的消息（例如读取属性值）。
     */
    public void onDeviceAttributesRequest(MqttPublishMessage mqttMsg) throws AdaptorException {
        if (isJsonPayloadType()) {
            onDeviceAttributesRequestJson(mqttMsg);
        } else {
            onDeviceAttributesRequestProto(mqttMsg);
        }
    }

    /**
     * 处理子设备RPC响应的消息。
     */
    public void onDeviceRpcResponse(MqttPublishMessage mqttMsg) throws AdaptorException {
        int msgId = getMsgId(mqttMsg);
        ByteBuf payload = mqttMsg.payload();
        if (isJsonPayloadType()) {
            onDeviceRpcResponseJson(msgId, payload);
        } else {
            onDeviceRpcResponseProto(msgId, payload);
        }
    }

    /**
     * 处理网关的心跳消息（例如通过`/gateway/ping`主题）。
     * 如果配置了覆盖子设备活动时间，则更新所有子设备的最后活动时间。
     */
    public void onGatewayPing() {
        if (overwriteDevicesActivity) {
            devices.forEach((deviceName, deviceSessionCtx) -> transportService.recordActivity(deviceSessionCtx.getSessionInfo()));
        }
    }

    /**
     * 网关断开连接时调用，清理所有子设备会话。
     * 等待正在创建中的设备Future完成，然后注销会话。
     */
    public void onDevicesDisconnect() {
        log.debug("[{}] Gateway disconnect [{}]", gateway.getTenantId(), gateway.getDeviceId());
        try {
            // 等待所有正在创建的设备完成后再注销
            deviceFutures.forEach((name, future) -> {
                Futures.addCallback(future, new FutureCallback<T>() {
                    @Override
                    public void onSuccess(T result) {
                        log.debug("[{}] Gateway disconnect [{}] device deregister callback [{}]", gateway.getTenantId(), gateway.getDeviceId(), name);
                        deregisterSession(name, result);
                    }

                    @Override
                    public void onFailure(Throwable t) {

                    }
                }, MoreExecutors.directExecutor());
            });
            // 注销所有已连接的设备
            devices.forEach(this::deregisterSession);
        } catch (Exception e) {
            log.error("Gateway disconnect failure", e);
        }
    }

    /**
     * 当子设备被删除时（例如从数据库删除），从网关会话中移除该设备。
     */
    public void onDeviceDeleted(String deviceName) {
        deregisterSession(deviceName);
    }

    /**
     * 获取当前节点的ID（用于分布式部署）。
     */
    public String getNodeId() {
        return context.getNodeId();
    }

    /**
     * 获取当前使用的负载适配器（JSON或Protobuf）。
     */
    public MqttTransportAdaptor getPayloadAdaptor() {
        return deviceSessionCtx.getPayloadAdaptor();
    }

    /**
     * 根据设备名注销子设备会话。
     */
    void deregisterSession(String deviceName) {
        MqttDeviceAwareSessionContext deviceSessionCtx = devices.remove(deviceName);
        if (deviceSessionCtx != null) {
            deregisterSession(deviceName, deviceSessionCtx);
        } else {
            log.debug("[{}][{}][{}] Device [{}] was already removed from the gateway session", gateway.getTenantId(), gateway.getDeviceId(), sessionId, deviceName);
        }
    }

    /**
     * 向客户端写入MQTT消息并冲刷。
     */
    public ChannelFuture writeAndFlush(MqttMessage mqttMessage) {
        return channel.writeAndFlush(mqttMessage);
    }

    /**
     * 获取下一个可用的消息ID（用于MQTT报文标识）。
     */
    int nextMsgId() {
        return deviceSessionCtx.nextMsgId();
    }

    /**
     * 判断网关使用的是否是JSON负载类型。
     */
    protected boolean isJsonPayloadType() {
        return deviceSessionCtx.isJsonPayloadType();
    }

    /**
     * 处理子设备连接的通用逻辑。
     * 调用onDeviceConnect异步获取设备上下文，成功后发送ACK，失败时记录日志。
     */
    protected void processOnConnect(MqttPublishMessage msg, String deviceName, String deviceType) {
        log.trace("[{}][{}][{}] onDeviceConnect: [{}]", gateway.getTenantId(), gateway.getDeviceId(), sessionId, deviceName);
        process(onDeviceConnect(deviceName, deviceType),
                result -> {
                    ack(msg, MqttReasonCodes.PubAck.SUCCESS);
                    log.trace("[{}][{}][{}] onDeviceConnectOk: [{}]", gateway.getTenantId(), gateway.getDeviceId(), sessionId, deviceName);
                },
                t -> logDeviceCreationError(t, deviceName));
    }

    /**
     * 当设备信息更新时调用（例如设备配置变化），可用于更新覆盖活动时间等属性。
     */
    public void onDeviceUpdate(TransportProtos.SessionInfoProto sessionInfo, Device device, Optional<DeviceProfile> deviceProfileOpt) {
        log.trace("[{}][{}] onDeviceUpdate: [{}]", gateway.getTenantId(), gateway.getDeviceId(), device);
        JsonNode deviceAdditionalInfo = device.getAdditionalInfo();
        if (deviceAdditionalInfo.has(DataConstants.GATEWAY_PARAMETER) && deviceAdditionalInfo.has(DataConstants.OVERWRITE_ACTIVITY_TIME_PARAMETER)) {
            overwriteDevicesActivity = deviceAdditionalInfo.get(DataConstants.OVERWRITE_ACTIVITY_TIME_PARAMETER).asBoolean();
        }
    }

    /**
     * 异步获取或创建子设备的会话上下文。
     * 使用双重检查锁和Future机制避免重复创建。
     *
     * @param deviceName 设备名
     * @param deviceType 设备类型
     * @return ListenableFuture<设备会话上下文>
     */
    ListenableFuture<T> onDeviceConnect(String deviceName, String deviceType) {
        T result = devices.get(deviceName);
        if (result == null) {
            // 获取或创建针对该设备名的锁
            Lock deviceCreationLock = deviceCreationLockMap.computeIfAbsent(deviceName, s -> new ReentrantLock());
            deviceCreationLock.lock();
            try {
                result = devices.get(deviceName);
                if (result == null) {
                    // 如果设备上下文还不存在，则发起异步创建请求
                    return getDeviceCreationFuture(deviceName, deviceType);
                } else {
                    return Futures.immediateFuture(result);
                }
            } finally {
                deviceCreationLock.unlock();
            }
        } else {
            return Futures.immediateFuture(result);
        }
    }

    /**
     * 创建设备会话的异步Future。
     * 向TransportService发送GetOrCreateDeviceFromGatewayRequest请求，
     * 成功后创建设备上下文，注册会话，并设置Future结果。
     */
    private ListenableFuture<T> getDeviceCreationFuture(String deviceName, String deviceType) {
        final SettableFuture<T> futureToSet = SettableFuture.create();
        ListenableFuture<T> future = deviceFutures.putIfAbsent(deviceName, futureToSet);
        if (future != null) {
            // 已经有正在创建的Future，直接返回
            return future;
        }
        try {
            // 构造请求消息，包含网关ID和设备信息
            transportService.process(gateway.getTenantId(),
                    GetOrCreateDeviceFromGatewayRequestMsg.newBuilder()
                            .setDeviceName(deviceName)
                            .setDeviceType(deviceType)
                            .setGatewayIdMSB(gateway.getDeviceId().getId().getMostSignificantBits())
                            .setGatewayIdLSB(gateway.getDeviceId().getId().getLeastSignificantBits())
                            .build(),
                    new TransportServiceCallback<>() {
                        @Override
                        public void onSuccess(GetOrCreateDeviceFromGatewayResponse msg) {
                            // 创建具体的设备会话上下文（由子类实现）
                            T deviceSessionCtx = newDeviceSessionCtx(msg);
                            if (devices.putIfAbsent(deviceName, deviceSessionCtx) == null) {
                                log.trace("[{}][{}][{}] First got or created device [{}], type [{}] for the gateway session", gateway.getTenantId(), gateway.getDeviceId(), sessionId, deviceName, deviceType);
                                SessionInfoProto deviceSessionInfo = deviceSessionCtx.getSessionInfo();
                                // 向传输服务注册异步会话
                                transportService.registerAsyncSession(deviceSessionInfo, deviceSessionCtx);
                                // 发送会话开启事件，并订阅属性更新和RPC
                                transportService.process(TransportProtos.TransportToDeviceActorMsg.newBuilder()
                                        .setSessionInfo(deviceSessionInfo)
                                        .setSessionEvent(SESSION_EVENT_MSG_OPEN)
                                        .setSubscribeToAttributes(SUBSCRIBE_TO_ATTRIBUTE_UPDATES_ASYNC_MSG)
                                        .setSubscribeToRPC(SUBSCRIBE_TO_RPC_ASYNC_MSG)
                                        .build(), null);
                            }
                            // 设置Future结果
                            futureToSet.set(devices.get(deviceName));
                            deviceFutures.remove(deviceName);
                        }

                        @Override
                        public void onError(Throwable t) {
                            logDeviceCreationError(t, deviceName);
                            futureToSet.setException(t);
                            deviceFutures.remove(deviceName);
                        }
                    });
            return futureToSet;
        } catch (Throwable e) {
            deviceFutures.remove(deviceName);
            throw e;
        }
    }

    /**
     * 记录设备创建失败的错误日志（区分达到设备数量上限的情况）。
     */
    private void logDeviceCreationError(Throwable t, String deviceName) {
        if (DataConstants.MAXIMUM_NUMBER_OF_DEVICES_REACHED.equals(t.getMessage())) {
            log.info("[{}][{}][{}] Failed to process device connect command: [{}] due to [{}]", gateway.getTenantId(), gateway.getDeviceId(), sessionId, deviceName,
                    DataConstants.MAXIMUM_NUMBER_OF_DEVICES_REACHED);
        } else {
            log.warn("[{}][{}][{}] Failed to process device connect command: [{}]", gateway.getTenantId(), gateway.getDeviceId(), sessionId, deviceName, t);
        }
    }

    /**
     * 创建新的子设备会话上下文，由子类实现。
     */
    protected abstract T newDeviceSessionCtx(GetOrCreateDeviceFromGatewayResponse msg);

    /**
     * 从MQTT发布消息中获取报文ID（packetId）。
     */
    protected int getMsgId(MqttPublishMessage mqttMsg) {
        return mqttMsg.variableHeader().packetId();
    }



    // ==================== JSON负载处理方法 ====================
    protected void onDeviceConnectJson(MqttPublishMessage mqttMsg) throws AdaptorException {
        JsonElement json = getJson(mqttMsg);
        String deviceName = checkDeviceName(getDeviceName(json));
        String deviceType = getDeviceType(json);
        processOnConnect(mqttMsg, deviceName, deviceType);
    }

    protected void onDeviceConnectProto(MqttPublishMessage mqttMsg) throws AdaptorException {
        try {
            TransportApiProtos.ConnectMsg connectProto = TransportApiProtos.ConnectMsg.parseFrom(getBytes(mqttMsg.payload()));
            String deviceName = checkDeviceName(connectProto.getDeviceName());
            String deviceType = StringUtils.isEmpty(connectProto.getDeviceType()) ? DEFAULT_DEVICE_TYPE : connectProto.getDeviceType();
            processOnConnect(mqttMsg, deviceName, deviceType);
        } catch (RuntimeException | InvalidProtocolBufferException e) {
            throw new AdaptorException(e);
        }
    }

    private void onDeviceDisconnectJson(MqttPublishMessage msg) throws AdaptorException {
        String deviceName = checkDeviceName(getDeviceName(getJson(msg)));
        processOnDisconnect(msg, deviceName);
    }

    protected void onGatewayDeviceDisconnectProto(MqttPublishMessage mqttMsg) throws AdaptorException {
        try {
            TransportApiProtos.DisconnectMsg connectProto = TransportApiProtos.DisconnectMsg.parseFrom(getBytes(mqttMsg.payload()));
            String deviceName = checkDeviceName(connectProto.getDeviceName());
            processOnDisconnect(mqttMsg, deviceName);
        } catch (RuntimeException | InvalidProtocolBufferException e) {
            throw new AdaptorException(e);
        }
    }

    /**
     * 处理设备断开连接的通用逻辑：从缓存中移除设备会话，并发送ACK。
     */
    void processOnDisconnect(MqttPublishMessage msg, String deviceName) {
        deregisterSession(deviceName);
        ack(msg, MqttReasonCodes.PubAck.SUCCESS);
    }

    /**
     * 处理JSON格式的遥测数据。
     * 格式：{ "deviceName": [ {ts:..., values:{...}} ] }
     */
    protected void onDeviceTelemetryJson(int msgId, ByteBuf payload) throws AdaptorException {
        JsonElement json = JsonMqttAdaptor.validateJsonPayload(sessionId, payload);
        validateJsonObject(json);
        for (Map.Entry<String, JsonElement> deviceEntry : json.getAsJsonObject().entrySet()) {
            if (!deviceEntry.getValue().isJsonArray()) {
                log.warn("{}[{}]", CAN_T_PARSE_VALUE, json);
                continue;
            }
            String deviceName = deviceEntry.getKey();
            // 异步处理遥测，防止阻塞
            process(deviceName, deviceCtx -> processPostTelemetryMsg(deviceCtx, deviceEntry.getValue(), deviceName, msgId),
                    t -> failedToProcessLog(deviceName, TELEMETRY, t));
        }
    }

    /**
     * 将JSON遥测数据转换为Protobuf消息并发送给TransportService。
     * 同时收集网关指标。
     */
    private void processPostTelemetryMsg(T deviceCtx, JsonElement msg, String deviceName, int msgId) {
        try {
            long systemTs = System.currentTimeMillis();
            TbPair<TransportProtos.PostTelemetryMsg, List<GatewayMetadata>> gatewayPayloadPair = JsonConverter.convertToGatewayTelemetry(msg.getAsJsonArray(), systemTs);
            TransportProtos.PostTelemetryMsg postTelemetryMsg = gatewayPayloadPair.getFirst();
            List<GatewayMetadata> metadata = gatewayPayloadPair.getSecond();
            if (!CollectionUtils.isEmpty(metadata)) {
                gatewayMetricsService.process(deviceSessionCtx.getSessionInfo(), gateway.getDeviceId(), metadata, systemTs);
            }
            transportService.process(deviceCtx.getSessionInfo(), postTelemetryMsg, getPubAckCallback(channel, deviceName, msgId, postTelemetryMsg));
        } catch (Throwable e) {
            log.warn("[{}][{}][{}] Failed to convert telemetry: [{}]", gateway.getTenantId(), gateway.getDeviceId(), deviceName, msg, e);
            ackOrClose(msgId);
        }
    }

    protected void onDeviceTelemetryProto(int msgId, ByteBuf payload) throws AdaptorException {
        try {
            TransportApiProtos.GatewayTelemetryMsg telemetryMsgProto = TransportApiProtos.GatewayTelemetryMsg.parseFrom(getBytes(payload));
            List<TransportApiProtos.TelemetryMsg> deviceMsgList = telemetryMsgProto.getMsgList();
            if (CollectionUtils.isEmpty(deviceMsgList)) {
                log.debug("[{}][{}][{}] Devices telemetry messages is empty", gateway.getTenantId(), gateway.getDeviceId(), sessionId);
                throw new IllegalArgumentException("[" + sessionId + "] Devices telemetry messages is empty for [" + gateway.getDeviceId() + "]");
            }

            deviceMsgList.forEach(telemetryMsg -> {
                String deviceName = checkDeviceName(telemetryMsg.getDeviceName());
                process(deviceName, deviceCtx -> processPostTelemetryMsg(deviceCtx, telemetryMsg.getMsg(), deviceName, msgId),
                        t -> failedToProcessLog(deviceName, TELEMETRY, t));
            });
        } catch (RuntimeException | InvalidProtocolBufferException e) {
            throw new AdaptorException(e);
        }
    }

    /**
     * 处理Protobuf格式的遥测数据（针对子设备）。
     */
    protected void processPostTelemetryMsg(MqttDeviceAwareSessionContext deviceCtx, TransportProtos.PostTelemetryMsg msg, String deviceName, int msgId) {
        try {
            TransportProtos.PostTelemetryMsg postTelemetryMsg = ProtoConverter.validatePostTelemetryMsg(msg.toByteArray());
            transportService.process(deviceCtx.getSessionInfo(), postTelemetryMsg, getPubAckCallback(channel, deviceName, msgId, postTelemetryMsg));
        } catch (Throwable e) {
            log.warn("[{}][{}][{}] Failed to convert telemetry: [{}]", gateway.getTenantId(), gateway.getDeviceId(), deviceName, msg, e);
            ackOrClose(msgId);
        }
    }

    /**
     * 创建一个包含单个KeyValue的PostTelemetryMsg（用于Sparkplug状态等）。
     */
    public TransportProtos.PostTelemetryMsg postTelemetryMsgCreated(TransportProtos.KeyValueProto keyValueProto, long ts) {
        List<TransportProtos.KeyValueProto> result = new ArrayList<>();
        result.add(keyValueProto);
        TransportProtos.PostTelemetryMsg.Builder request = TransportProtos.PostTelemetryMsg.newBuilder();
        TransportProtos.TsKvListProto.Builder builder = TransportProtos.TsKvListProto.newBuilder();
        builder.setTs(ts);
        builder.addAllKv(result);
        request.addTsKvList(builder.build());
        return request.build();
    }

    // ==================== 设备认领处理 ====================
    private void onDeviceClaimJson(int msgId, ByteBuf payload) throws AdaptorException {
        JsonElement json = JsonMqttAdaptor.validateJsonPayload(sessionId, payload);
        validateJsonObject(json);
        for (Map.Entry<String, JsonElement> deviceEntry : json.getAsJsonObject().entrySet()) {
            if (!deviceEntry.getValue().isJsonObject()) {
                log.warn("{}[{}]", CAN_T_PARSE_VALUE, json);
                continue;
            }

            String deviceName = deviceEntry.getKey();
            process(deviceName, deviceCtx -> processClaimDeviceMsg(deviceCtx, deviceEntry.getValue(), deviceName, msgId),
                    t -> failedToProcessLog(deviceName, CLAIMING, t));
        }
    }

    private void processClaimDeviceMsg(MqttDeviceAwareSessionContext deviceCtx, JsonElement claimRequest, String deviceName, int msgId) {
        try {
            DeviceId deviceId = deviceCtx.getDeviceId();
            TransportProtos.ClaimDeviceMsg claimDeviceMsg = JsonConverter.convertToClaimDeviceProto(deviceId, claimRequest);
            transportService.process(deviceCtx.getSessionInfo(), claimDeviceMsg, getPubAckCallback(channel, deviceName, msgId, claimDeviceMsg));
        } catch (Throwable e) {
            log.warn("[{}][{}][{}] Failed to convert claim message: [{}]", gateway.getTenantId(), gateway.getDeviceId(), deviceName, claimRequest, e);
            ackOrClose(msgId);
        }
    }

    private void onDeviceClaimProto(int msgId, ByteBuf payload) throws AdaptorException {
        try {
            TransportApiProtos.GatewayClaimMsg claimMsgProto = TransportApiProtos.GatewayClaimMsg.parseFrom(getBytes(payload));
            List<TransportApiProtos.ClaimDeviceMsg> claimMsgList = claimMsgProto.getMsgList();
            if (CollectionUtils.isEmpty(claimMsgList)) {
                log.debug("[{}][{}][{}] Devices claim messages is empty", gateway.getTenantId(), gateway.getDeviceId(), sessionId);
                throw new IllegalArgumentException("[" + sessionId + "] Devices claim messages is empty for [" + gateway.getDeviceId() + "]");
            }

            claimMsgList.forEach(claimDeviceMsg -> {
                String deviceName = checkDeviceName(claimDeviceMsg.getDeviceName());
                process(deviceName, deviceCtx -> processClaimDeviceMsg(deviceCtx, claimDeviceMsg.getClaimRequest(), deviceName, msgId),
                        t -> failedToProcessLog(deviceName, CLAIMING, t));
            });
        } catch (RuntimeException | InvalidProtocolBufferException e) {
            throw new AdaptorException(e);
        }
    }

    private void processClaimDeviceMsg(MqttDeviceAwareSessionContext deviceCtx, TransportApiProtos.ClaimDevice claimRequest, String deviceName, int msgId) {
        try {
            DeviceId deviceId = deviceCtx.getDeviceId();
            TransportProtos.ClaimDeviceMsg claimDeviceMsg = ProtoConverter.convertToClaimDeviceProto(deviceId, claimRequest.toByteArray());
            transportService.process(deviceCtx.getSessionInfo(), claimDeviceMsg, getPubAckCallback(channel, deviceName, msgId, claimDeviceMsg));
        } catch (Throwable e) {
            log.warn("[{}][{}][{}] Failed to convert claim message: [{}]", gateway.getTenantId(), gateway.getDeviceId(), deviceName, claimRequest, e);
            ackOrClose(msgId);
        }
    }

    // ==================== 设备属性处理 ====================
    private void onDeviceAttributesJson(int msgId, ByteBuf payload) throws AdaptorException {
        JsonElement json = JsonMqttAdaptor.validateJsonPayload(sessionId, payload);
        validateJsonObject(json);
        for (Map.Entry<String, JsonElement> deviceEntry : json.getAsJsonObject().entrySet()) {
            if (!deviceEntry.getValue().isJsonObject()) {
                log.warn("{}[{}]", CAN_T_PARSE_VALUE, json);
                continue;
            }

            String deviceName = deviceEntry.getKey();
            process(deviceName, deviceCtx -> processPostAttributesMsg(deviceCtx, deviceEntry.getValue(), deviceName, msgId),
                    t -> failedToProcessLog(deviceName, ATTRIBUTE, t));
        }
    }

    private void processPostAttributesMsg(MqttDeviceAwareSessionContext deviceCtx, JsonElement msg, String deviceName, int msgId) {
        try {
            TransportProtos.PostAttributeMsg postAttributeMsg = JsonConverter.convertToAttributesProto(msg.getAsJsonObject());
            transportService.process(deviceCtx.getSessionInfo(), postAttributeMsg, getPubAckCallback(channel, deviceName, msgId, postAttributeMsg));
        } catch (Throwable e) {
            log.warn("[{}][{}][{}] Failed to process device attributes command: [{}]", gateway.getTenantId(), gateway.getDeviceId(), deviceName, msg, e);
            ackOrClose(msgId);
        }
    }

    private void onDeviceAttributesProto(int msgId, ByteBuf payload) throws AdaptorException {
        try {
            TransportApiProtos.GatewayAttributesMsg attributesMsgProto = TransportApiProtos.GatewayAttributesMsg.parseFrom(getBytes(payload));
            List<TransportApiProtos.AttributesMsg> attributesMsgList = attributesMsgProto.getMsgList();
            if (CollectionUtils.isEmpty(attributesMsgList)) {
                log.debug("[{}][{}][{}] Devices attributes keys list is empty", gateway.getTenantId(), gateway.getDeviceId(), sessionId);
                throw new IllegalArgumentException("[" + sessionId + "] Devices attributes keys list is empty for [" + gateway.getDeviceId() + "]");
            }

            attributesMsgList.forEach(attributesMsg -> {
                String deviceName = checkDeviceName(attributesMsg.getDeviceName());
                process(deviceName, deviceCtx -> processPostAttributesMsg(deviceCtx, attributesMsg.getMsg(), deviceName, msgId),
                        t -> failedToProcessLog(deviceName, ATTRIBUTE, t));
            });
        } catch (RuntimeException | InvalidProtocolBufferException e) {
            throw new AdaptorException(e);
        }
    }

    protected void processPostAttributesMsg(MqttDeviceAwareSessionContext deviceCtx, TransportProtos.PostAttributeMsg kvListProto, String deviceName, int msgId) {
        try {
            TransportProtos.PostAttributeMsg postAttributeMsg = ProtoConverter.validatePostAttributeMsg(kvListProto);
            transportService.process(deviceCtx.getSessionInfo(), postAttributeMsg, getPubAckCallback(channel, deviceName, msgId, postAttributeMsg));
        } catch (Throwable e) {
            log.warn("[{}][{}][{}] Failed to process device attributes command: [{}]", gateway.getTenantId(), gateway.getDeviceId(), deviceName, kvListProto, e);
            ackOrClose(msgId);
        }
    }

    // ==================== 设备属性请求处理 ====================
    private void onDeviceAttributesRequestJson(MqttPublishMessage msg) throws AdaptorException {
        JsonElement json = JsonMqttAdaptor.validateJsonPayload(sessionId, msg.payload());
        validateJsonObject(json);
        JsonObject jsonObj = json.getAsJsonObject();
        int requestId = jsonObj.get("id").getAsInt();
        String deviceName = jsonObj.get(DEVICE_PROPERTY).getAsString();
        boolean clientScope = jsonObj.get("client").getAsBoolean();
        Set<String> keys;
        if (jsonObj.has("key")) {
            keys = Collections.singleton(jsonObj.get("key").getAsString());
        } else {
            JsonArray keysArray = jsonObj.get("keys").getAsJsonArray();
            keys = new HashSet<>();
            for (JsonElement keyObj : keysArray) {
                keys.add(keyObj.getAsString());
            }
        }
        TransportProtos.GetAttributeRequestMsg requestMsg = toGetAttributeRequestMsg(requestId, clientScope, keys);
        processGetAttributeRequestMessage(msg, deviceName, requestMsg);
    }

    private void onDeviceAttributesRequestProto(MqttPublishMessage mqttMsg) throws AdaptorException {
        try {
            TransportApiProtos.GatewayAttributesRequestMsg gatewayAttributesRequestMsg = TransportApiProtos.GatewayAttributesRequestMsg.parseFrom(getBytes(mqttMsg.payload()));
            String deviceName = checkDeviceName(gatewayAttributesRequestMsg.getDeviceName());
            int requestId = gatewayAttributesRequestMsg.getId();
            boolean clientScope = gatewayAttributesRequestMsg.getClient();
            ProtocolStringList keysList = gatewayAttributesRequestMsg.getKeysList();
            Set<String> keys = new HashSet<>(keysList);
            TransportProtos.GetAttributeRequestMsg requestMsg = toGetAttributeRequestMsg(requestId, clientScope, keys);
            processGetAttributeRequestMessage(mqttMsg, deviceName, requestMsg);
        } catch (RuntimeException | InvalidProtocolBufferException e) {
            throw new AdaptorException(e);
        }
    }

    // ==================== 设备RPC响应处理 ====================
    private void onDeviceRpcResponseJson(int msgId, ByteBuf payload) throws AdaptorException {
        JsonElement json = JsonMqttAdaptor.validateJsonPayload(sessionId, payload);
        validateJsonObject(json);
        JsonObject jsonObj = json.getAsJsonObject();
        String deviceName = jsonObj.get(DEVICE_PROPERTY).getAsString();
        Integer requestId = jsonObj.get("id").getAsInt();
        String data = jsonObj.get("data").toString();
        onDeviceRpcResponse(requestId, data, deviceName, msgId);
    }

    private static void validateJsonObject(JsonElement json) {
        if (!json.isJsonObject()) {
            throw new JsonSyntaxException(CAN_T_PARSE_VALUE + json);
        }
    }

    private void onDeviceRpcResponseProto(int msgId, ByteBuf payload) throws AdaptorException {
        try {
            TransportApiProtos.GatewayRpcResponseMsg gatewayRpcResponseMsg = TransportApiProtos.GatewayRpcResponseMsg.parseFrom(getBytes(payload));
            String deviceName = checkDeviceName(gatewayRpcResponseMsg.getDeviceName());
            Integer requestId = gatewayRpcResponseMsg.getId();
            String data = gatewayRpcResponseMsg.getData();
            onDeviceRpcResponse(requestId, data, deviceName, msgId);
        } catch (RuntimeException | InvalidProtocolBufferException e) {
            throw new AdaptorException(e);
        }
    }

    private void onDeviceRpcResponse(Integer requestId, String data, String deviceName, int msgId) {
        process(deviceName, deviceCtx -> processRpcResponseMsg(deviceCtx, requestId, data, deviceName, msgId),
                t -> failedToProcessLog(deviceName, RPC_RESPONSE, t));
    }

    private void processRpcResponseMsg(MqttDeviceAwareSessionContext deviceCtx, Integer requestId, String data, String deviceName, int msgId) {
        TransportProtos.ToDeviceRpcResponseMsg rpcResponseMsg = TransportProtos.ToDeviceRpcResponseMsg.newBuilder()
                .setRequestId(requestId).setPayload(data).build();
        transportService.process(deviceCtx.getSessionInfo(), rpcResponseMsg, getPubAckCallback(channel, deviceName, msgId, rpcResponseMsg));
    }

    // ==================== 通用处理辅助方法 ====================
    private void processGetAttributeRequestMessage(MqttPublishMessage mqttMsg, String deviceName, TransportProtos.GetAttributeRequestMsg requestMsg) {
        int msgId = getMsgId(mqttMsg);
        process(deviceName, deviceCtx -> processGetAttributeRequestMessage(deviceCtx, requestMsg, deviceName, msgId),
                t -> {
                    failedToProcessLog(deviceName, ATTRIBUTES_REQUEST, t);
                    ack(mqttMsg, MqttReasonCodes.PubAck.IMPLEMENTATION_SPECIFIC_ERROR);
                });
    }

    private void processGetAttributeRequestMessage(T deviceCtx, TransportProtos.GetAttributeRequestMsg requestMsg, String deviceName, int msgId) {
        transportService.process(deviceCtx.getSessionInfo(), requestMsg, getPubAckCallback(channel, deviceName, msgId, requestMsg));
    }

    private TransportProtos.GetAttributeRequestMsg toGetAttributeRequestMsg(int requestId, boolean clientScope, Set<String> keys) {
        TransportProtos.GetAttributeRequestMsg.Builder result = TransportProtos.GetAttributeRequestMsg.newBuilder();
        result.setRequestId(requestId);

        if (clientScope) {
            result.addAllClientAttributeNames(keys);
        } else {
            result.addAllSharedAttributeNames(keys);
        }
        return result.build();
    }

    protected String checkDeviceName(String deviceName) {
        if (StringUtils.isEmpty(deviceName)) {
            throw new RuntimeException("Device name is empty!");
        } else {
            return deviceName;
        }
    }

    private String getDeviceName(JsonElement json) {
        return json.getAsJsonObject().get(DEVICE_PROPERTY).getAsString();
    }

    private String getDeviceType(JsonElement json) {
        JsonElement type = json.getAsJsonObject().get("type");
        return type == null || type instanceof JsonNull ? DEFAULT_DEVICE_TYPE : type.getAsString();
    }

    private JsonElement getJson(MqttPublishMessage mqttMsg) throws AdaptorException {
        return JsonMqttAdaptor.validateJsonPayload(sessionId, mqttMsg.payload());
    }

    protected byte[] getBytes(ByteBuf payload) {
        return ProtoMqttAdaptor.toBytes(payload);
    }

    /**
     * 发送MQTT PUBACK确认消息。
     */
    protected void ack(MqttPublishMessage msg, MqttReasonCodes.PubAck returnCode) {
        int msgId = getMsgId(msg);
        ack(msgId, returnCode);
    }

    protected void ack(int msgId, MqttReasonCodes.PubAck returnCode) {
        if (msgId > 0) {
            writeAndFlush(MqttTransportHandler.createMqttPubAckMsg(deviceSessionCtx, msgId, returnCode.byteValue()));
        }
    }

    /**
     * 根据MQTT版本决定ACK或关闭连接（用于解析失败等情况）。
     */
    protected void ackOrClose(int msgId) {
        if (MqttVersion.MQTT_5.equals(deviceSessionCtx.getMqttVersion())) {
            ack(msgId, MqttReasonCodes.PubAck.PAYLOAD_FORMAT_INVALID);
        } else {
            channel.close();
        }
    }

    /**
     * 注销子设备会话，发送关闭事件，并处理Sparkplug离线状态。
     */
    private void deregisterSession(String deviceName, MqttDeviceAwareSessionContext deviceSessionCtx) {
        if (this.deviceSessionCtx.isSparkplug()) {
            sendSparkplugStateOnTelemetry(deviceSessionCtx.getSessionInfo(),
                    deviceSessionCtx.getDeviceInfo().getDeviceName(), OFFLINE, new Date().getTime());
        }
        transportService.deregisterSession(deviceSessionCtx.getSessionInfo());
        transportService.process(deviceSessionCtx.getSessionInfo(), SESSION_EVENT_MSG_CLOSED, null);
        log.debug("[{}][{}][{}] Removed device [{}] from the gateway session", gateway.getTenantId(), gateway.getDeviceId(), sessionId, deviceName);
    }

    /**
     * 发送Sparkplug连接状态作为遥测（用于出生/死亡消息）。
     */
    public void sendSparkplugStateOnTelemetry(TransportProtos.SessionInfoProto sessionInfo, String deviceName, SparkplugConnectionState connectionState, long ts) {
        TransportProtos.KeyValueProto.Builder keyValueProtoBuilder = TransportProtos.KeyValueProto.newBuilder();
        keyValueProtoBuilder.setKey(messageName(STATE));
        keyValueProtoBuilder.setType(TransportProtos.KeyValueType.STRING_V);
        keyValueProtoBuilder.setStringV(connectionState.name());
        TransportProtos.PostTelemetryMsg postTelemetryMsg = postTelemetryMsgCreated(keyValueProtoBuilder.build(), ts);
        transportService.process(sessionInfo, postTelemetryMsg, getPubAckCallback(channel, deviceName, -1, postTelemetryMsg));
    }

    /**
     * 获取PUBACK回调，用于处理消息发送成功或失败后的动作。
     */
    private <T> TransportServiceCallback<Void> getPubAckCallback(final ChannelHandlerContext ctx, final String deviceName, final int msgId, final T msg) {
        return new TransportServiceCallback<Void>() {
            @Override
            public void onSuccess(Void dummy) {
                log.trace("[{}][{}][{}][{}] Published msg: [{}]", gateway.getTenantId(), gateway.getDeviceId(), sessionId, deviceName, msg);
                if (msgId > 0) {
                    ctx.writeAndFlush(MqttTransportHandler.createMqttPubAckMsg(deviceSessionCtx, msgId, MqttReasonCodes.PubAck.SUCCESS.byteValue()));
                } else {
                    log.trace("[{}][{}][{}] Wrong msg id: [{}]", gateway.getTenantId(), gateway.getDeviceId(), sessionId, msg);
                    ctx.writeAndFlush(MqttTransportHandler.createMqttPubAckMsg(deviceSessionCtx, msgId, MqttReasonCodes.PubAck.UNSPECIFIED_ERROR.byteValue()));
                    closeDeviceSession(deviceName, MqttReasonCodes.Disconnect.MALFORMED_PACKET);
                }
            }

            @Override
            public void onError(Throwable e) {
                log.trace("[{}][{}][{}] Failed to publish msg: [{}] for device: [{}]", gateway.getTenantId(), gateway.getDeviceId(), sessionId, msg, deviceName, e);
                if (e instanceof TbRateLimitsException) {
                    closeDeviceSession(deviceName, MqttReasonCodes.Disconnect.MESSAGE_RATE_TOO_HIGH);
                } else {
                    closeDeviceSession(deviceName, MqttReasonCodes.Disconnect.UNSPECIFIED_ERROR);
                }
                ctx.close();
            }
        };
    }

    /**
     * 处理设备消息的通用模板：先获取设备上下文，再执行成功回调，失败时执行失败回调。
     */
    protected void process(String deviceName, Consumer<T> onSuccess, Consumer<Throwable> onFailure) {
        ListenableFuture<T> deviceCtxFuture = onDeviceConnect(deviceName, DEFAULT_DEVICE_TYPE);
        process(deviceCtxFuture, onSuccess, onFailure);
    }

    @SneakyThrows
    protected <T> void process(ListenableFuture<T> deviceCtxFuture, Consumer<T> onSuccess, Consumer<Throwable> onFailure) {
        if (deviceCtxFuture.isDone()) {
            onSuccess.accept(deviceCtxFuture.get());
        } else {
            DonAsynchron.withCallback(deviceCtxFuture, onSuccess, onFailure, context.getExecutor());
        }
    }

    protected void failedToProcessLog(String deviceName, String msgType, Throwable t) {
        log.debug("[{}][{}][{}] Failed to process device {} command: [{}]", gateway.getTenantId(), gateway.getDeviceId(), sessionId, msgType, deviceName, t);
    }


    /**
     * 关闭子设备会话（MQTT 5中可能发送DISCONNECT消息给网关）。
     */
    private void closeDeviceSession(String deviceName, MqttReasonCodes.Disconnect returnCode) {
        try {
            if (MqttVersion.MQTT_5.equals(deviceSessionCtx.getMqttVersion())) {
                MqttTransportAdaptor adaptor = deviceSessionCtx.getPayloadAdaptor();
                int returnCodeValue = returnCode.byteValue() & 0xFF;
                Optional<MqttMessage> deviceDisconnectPublishMsg = adaptor.convertToGatewayDeviceDisconnectPublish(deviceSessionCtx, deviceName, returnCodeValue);
                deviceDisconnectPublishMsg.ifPresent(deviceSessionCtx.getChannel()::writeAndFlush);
            }
        } catch (Exception e) {
            log.trace("Failed to send device disconnect to gateway session", e);
        }
    }
}
