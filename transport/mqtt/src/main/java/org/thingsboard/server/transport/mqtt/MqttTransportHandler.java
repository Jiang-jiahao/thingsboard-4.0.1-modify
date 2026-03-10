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
package org.thingsboard.server.transport.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonParseException;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.leshan.core.ResponseCode;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.adaptor.AdaptorException;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.TransportPayloadType;
import org.thingsboard.server.common.data.device.profile.MqttTopics;
import org.thingsboard.server.common.data.exception.ThingsboardErrorCode;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.OtaPackageId;
import org.thingsboard.server.common.data.ota.OtaPackageType;
import org.thingsboard.server.common.data.rpc.RpcStatus;
import org.thingsboard.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import org.thingsboard.server.common.msg.EncryptionUtil;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.msg.tools.TbRateLimitsException;
import org.thingsboard.server.common.transport.SessionMsgListener;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.auth.SessionInfoCreator;
import org.thingsboard.server.common.transport.auth.TransportDeviceInfo;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.common.transport.service.SessionMetaData;
import org.thingsboard.server.common.transport.util.SslUtil;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.ProvisionDeviceResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ValidateDeviceX509CertRequestMsg;
import org.thingsboard.server.gen.transport.mqtt.SparkplugBProto;
import org.thingsboard.server.queue.scheduler.SchedulerComponent;
import org.thingsboard.server.transport.mqtt.adaptors.MqttTransportAdaptor;
import org.thingsboard.server.transport.mqtt.adaptors.ProtoMqttAdaptor;
import org.thingsboard.server.transport.mqtt.limits.GatewaySessionLimits;
import org.thingsboard.server.transport.mqtt.limits.SessionLimits;
import org.thingsboard.server.transport.mqtt.session.DeviceSessionCtx;
import org.thingsboard.server.transport.mqtt.session.GatewaySessionHandler;
import org.thingsboard.server.transport.mqtt.session.MqttTopicMatcher;
import org.thingsboard.server.transport.mqtt.session.SparkplugNodeSessionHandler;
import org.thingsboard.server.transport.mqtt.util.ReturnCodeResolver;
import org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMessageType;
import org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugRpcRequestHeader;
import org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugRpcResponseBody;
import org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugTopic;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.netty.handler.codec.mqtt.MqttMessageType.CONNECT;
import static io.netty.handler.codec.mqtt.MqttMessageType.PINGRESP;
import static io.netty.handler.codec.mqtt.MqttMessageType.SUBACK;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static org.thingsboard.server.common.transport.service.DefaultTransportService.SESSION_EVENT_MSG_CLOSED;
import static org.thingsboard.server.common.transport.service.DefaultTransportService.SESSION_EVENT_MSG_OPEN;
import static org.thingsboard.server.common.transport.service.DefaultTransportService.SUBSCRIBE_TO_ATTRIBUTE_UPDATES_ASYNC_MSG;
import static org.thingsboard.server.common.transport.service.DefaultTransportService.SUBSCRIBE_TO_RPC_ASYNC_MSG;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugConnectionState.OFFLINE;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMessageType.NDEATH;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMetricUtil.getTsKvProto;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugTopicUtil.parseTopicPublish;

/**
 * @author Andrew Shvayka
 */
@Slf4j
public class MqttTransportHandler extends ChannelInboundHandlerAdapter implements GenericFutureListener<Future<? super Void>>, SessionMsgListener {

    private static final Pattern FW_REQUEST_PATTERN = Pattern.compile(MqttTopics.DEVICE_FIRMWARE_REQUEST_TOPIC_PATTERN);
    private static final Pattern SW_REQUEST_PATTERN = Pattern.compile(MqttTopics.DEVICE_SOFTWARE_REQUEST_TOPIC_PATTERN);

    /**
     * RPC方法名：获取会话限制
     */
    private static final String SESSION_LIMITS = "getSessionLimits";

    /**
     * OTA包错误信息
     */
    private static final String PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE";

    /**
     * 支持的最大QoS等级
     */
    private static final MqttQoS MAX_SUPPORTED_QOS_LVL = AT_LEAST_ONCE;

    /**
     * 当前会话的唯一标识
     */
    private final UUID sessionId;

    /**
     * MQTT传输上下文（配置、服务等）
     */
    protected final MqttTransportContext context;

    /**
     * 传输服务，用于与核心服务通信
      */
    private final TransportService transportService;

    /**
     * 调度器组件
     */
    private final SchedulerComponent scheduler;

    /**
     * SSL处理器（如果启用SSL）
     */
    private final SslHandler sslHandler;

    /**
     * 主题订阅的QoS映射
     */
    private final ConcurrentMap<MqttTopicMatcher, Integer> mqttQoSMap;

    /**
     * 设备会话上下文（存储会话相关数据）
     */
    final DeviceSessionCtx deviceSessionCtx;

    /**
     * 客户端地址
     */
    volatile InetSocketAddress address;

    /**
     * 网关会话处理器（如果设备是网关）
     */
    volatile GatewaySessionHandler gatewaySessionHandler;

    /**
     * Sparkplug节点会话处理器
     */
    volatile SparkplugNodeSessionHandler sparkplugSessionHandler;

    /**
     * OTA包相关：存储请求ID与包ID的映射
     */
    private final ConcurrentHashMap<String, String> otaPackSessions;

    /**
     * 存储每个OTA请求的块大小
     */
    private final ConcurrentHashMap<String, Integer> chunkSizes;

    /**
     * 等待ACK的RPC请求（key: MQTT消息ID, value: RPC请求消息）
     */
    private final ConcurrentMap<Integer, TransportProtos.ToDeviceRpcRequestMsg> rpcAwaitingAck;

    /**
     * 主题类型枚举：标识不同的MQTT主题版本和格式
     */
    private TopicType attrSubTopicType; // 属性订阅主题类型
    private TopicType rpcSubTopicType; // RPC订阅主题类型
    private TopicType attrReqTopicType; // 属性请求主题类型
    private TopicType toServerRpcSubTopicType; // 服务端RPC响应主题类型

    MqttTransportHandler(MqttTransportContext context, SslHandler sslHandler) {
        this.sessionId = UUID.randomUUID();
        this.context = context;
        this.transportService = context.getTransportService();
        this.scheduler = context.getScheduler();
        this.sslHandler = sslHandler;
        this.mqttQoSMap = new ConcurrentHashMap<>();
        this.deviceSessionCtx = new DeviceSessionCtx(sessionId, mqttQoSMap, context);
        this.otaPackSessions = new ConcurrentHashMap<>();
        this.chunkSizes = new ConcurrentHashMap<>();
        this.rpcAwaitingAck = new ConcurrentHashMap<>();
    }

    /**
     * 通道注册时调用 - 统计连接数
     */
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        // 使用context（这个对象是全局唯一的，避免因为handler多实例的情况下导致记录错误）来记录当前连接数
        context.channelRegistered();
    }

    /**
     * 通道注销时调用 - 统计连接数
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        context.channelUnregistered();
    }

    /**
     * 读取通道数据 - 处理接收到的消息
     * @param ctx ChannelHandler上下文
     * @param msg 接收到的消息对象
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.trace("[{}] Processing msg: {}", sessionId, msg);
        // 获取真实设备的ip和端口
        if (address == null) {
            address = getAddress(ctx);
        }
        try {
            if (msg instanceof MqttMessage) {
                MqttMessage message = (MqttMessage) msg;
                if (message.decoderResult().isSuccess()) {
                    // 处理MQTT消息
                    processMqttMsg(ctx, message);
                } else {
                    log.error("[{}] Message decoding failed: {}", sessionId, message.decoderResult().cause().getMessage());
                    closeCtx(ctx, MqttReasonCodes.Disconnect.MALFORMED_PACKET);
                }
            } else {
                // 非MQTT消息，关闭连接
                log.debug("[{}] Received non mqtt message: {}", sessionId, msg.getClass().getSimpleName());
                closeCtx(ctx, (MqttMessage) null);
            }
        } finally {
            ReferenceCountUtil.safeRelease(msg);
        }
    }

    private void closeCtx(ChannelHandlerContext ctx, MqttReasonCodes.Disconnect returnCode) {
        closeCtx(ctx, returnCode.byteValue());
    }

    private void closeCtx(ChannelHandlerContext ctx, MqttConnectReturnCode returnCode) {
        closeCtx(ctx, ReturnCodeResolver.getConnectionReturnCode(deviceSessionCtx.getMqttVersion(), returnCode).byteValue());
    }

    private void closeCtx(ChannelHandlerContext ctx, byte returnCode) {
        closeCtx(ctx, createMqttDisconnectMsg(deviceSessionCtx, returnCode));
    }

    private void closeCtx(ChannelHandlerContext ctx, MqttMessage msg) {
        if (!rpcAwaitingAck.isEmpty()) {
            log.debug("[{}] Cleanup RPC awaiting ack map due to session close!", sessionId);
            rpcAwaitingAck.clear();
        }

        if (ctx.channel() == null) {
            log.debug("[{}] Channel is null, closing ctx...", sessionId);
            ctx.close();
        } else if (ctx.channel().isOpen()) {
            if (msg != null && MqttVersion.MQTT_5 == deviceSessionCtx.getMqttVersion()) {
                ChannelFuture channelFuture = ctx.writeAndFlush(msg).addListener(future -> ctx.close());
                scheduler.schedule(() -> {
                    if (!channelFuture.isDone()) {
                        log.debug("[{}] Closing channel due to timeout!", sessionId);
                        ctx.close();
                    }
                }, context.getDisconnectTimeout(), TimeUnit.MILLISECONDS);
            } else {
                ctx.close();
            }
        } else {
            log.debug("[{}] Channel is already closed!", sessionId);
        }
    }

    InetSocketAddress getAddress(ChannelHandlerContext ctx) {
        var address = ctx.channel().attr(MqttTransportService.ADDRESS).get();
        if (address == null) {
            log.trace("[{}] Received empty address.", ctx.channel().id());
            InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            log.trace("[{}] Going to use address: {}", ctx.channel().id(), remoteAddress);
            return remoteAddress;
        } else {
            log.trace("[{}] Received address: {}", ctx.channel().id(), address);
        }
        return address;
    }

    /**
     * 处理MQTT消息 - 根据消息类型路由到不同处理方法
     * @param ctx ChannelHandler上下文
     * @param msg MQTT消息
     */
    void processMqttMsg(ChannelHandlerContext ctx, MqttMessage msg) {
        if (msg.fixedHeader() == null) {
            log.info("[{}:{}] Invalid message received", address.getHostName(), address.getPort());
            closeCtx(ctx, MqttReasonCodes.Disconnect.PROTOCOL_ERROR);
            return;
        }
        // 设置当前通道到会话上下文
        deviceSessionCtx.setChannel(ctx);
        if (CONNECT.equals(msg.fixedHeader().messageType())) {
            // 处理连接请求
            processConnect(ctx, (MqttConnectMessage) msg);
        } else if (deviceSessionCtx.isProvisionOnly()) {
            // 处理设备预配置会话消息（只允许预配置相关操作）
            processProvisionSessionMsg(ctx, msg);
        } else {
            // 处理常规设备会话消息（放入队列异步处理）
            enqueueRegularSessionMsg(ctx, msg);
        }
    }

    /**
     * 处理设备预配置会话消息
     * 预配置模式下只允许特定的操作：发布预配置请求、订阅预配置响应等
     */
    private void processProvisionSessionMsg(ChannelHandlerContext ctx, MqttMessage msg) {
        switch (msg.fixedHeader().messageType()) {
            case PUBLISH:
                // 处理发布消息
                MqttPublishMessage mqttMsg = (MqttPublishMessage) msg;
                String topicName = mqttMsg.variableHeader().topicName();
                int msgId = mqttMsg.variableHeader().packetId();
                try {
                    if (topicName.equals(MqttTopics.DEVICE_PROVISION_REQUEST_TOPIC)) {
                        try {
                            // 处理设备预配置请求
                            TransportProtos.ProvisionDeviceRequestMsg provisionRequestMsg = deviceSessionCtx.getContext().getJsonMqttAdaptor().convertToProvisionRequestMsg(deviceSessionCtx, mqttMsg);
                            transportService.process(provisionRequestMsg, new DeviceProvisionCallback(ctx, msgId, provisionRequestMsg));
                            log.trace("[{}][{}] Processing provision publish msg [{}][{}]!", sessionId, deviceSessionCtx.getDeviceId(), topicName, msgId);
                        } catch (Exception e) {
                            // 如果JSON解析失败，尝试使用Protobuf格式
                            if (e instanceof JsonParseException || (e.getCause() != null && e.getCause() instanceof JsonParseException)) {
                                TransportProtos.ProvisionDeviceRequestMsg provisionRequestMsg = deviceSessionCtx.getContext().getProtoMqttAdaptor().convertToProvisionRequestMsg(deviceSessionCtx, mqttMsg);
                                transportService.process(provisionRequestMsg, new DeviceProvisionCallback(ctx, msgId, provisionRequestMsg));
                                deviceSessionCtx.setProvisionPayloadType(TransportPayloadType.PROTOBUF);
                                log.trace("[{}][{}] Processing provision publish msg [{}][{}]!", sessionId, deviceSessionCtx.getDeviceId(), topicName, msgId);
                            } else {
                                throw e;
                            }
                        }
                    } else {
                        log.debug("[{}] Unsupported topic for provisioning requests: {}!", sessionId, topicName);
                        ack(ctx, msgId, MqttReasonCodes.PubAck.TOPIC_NAME_INVALID);
                        closeCtx(ctx, MqttReasonCodes.Disconnect.TOPIC_NAME_INVALID);
                    }
                } catch (RuntimeException e) {
                    log.warn("[{}] Failed to process publish msg [{}][{}]", sessionId, topicName, msgId, e);
                    ack(ctx, msgId, MqttReasonCodes.PubAck.IMPLEMENTATION_SPECIFIC_ERROR);
                    closeCtx(ctx, MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR);
                } catch (AdaptorException e) {
                    log.debug("[{}] Failed to process publish msg [{}][{}]", sessionId, topicName, msgId, e);
                    sendResponseForAdaptorErrorOrCloseContext(ctx, topicName, msgId);
                }
                break;
            case SUBSCRIBE:
                // 处理订阅请求
                MqttSubscribeMessage subscribeMessage = (MqttSubscribeMessage) msg;
                processSubscribe(ctx, subscribeMessage);
                break;
            case UNSUBSCRIBE:
                // 处理取消订阅
                MqttUnsubscribeMessage unsubscribeMessage = (MqttUnsubscribeMessage) msg;
                processUnsubscribe(ctx, unsubscribeMessage);
                break;
            case PINGREQ:
                // 响应心跳
                ctx.writeAndFlush(new MqttMessage(new MqttFixedHeader(PINGRESP, false, AT_MOST_ONCE, false, 0)));
                break;
            case DISCONNECT:
                // 处理断开连接
                closeCtx(ctx, MqttReasonCodes.Disconnect.NORMAL_DISCONNECT);
                break;
        }
    }

    /**
     * 将常规会话消息放入队列进行异步处理
     * 防止设备连接过程中消息处理阻塞
     */
    void enqueueRegularSessionMsg(ChannelHandlerContext ctx, MqttMessage msg) {
        final int queueSize = deviceSessionCtx.getMsgQueueSize();
        if (queueSize >= context.getMessageQueueSizePerDeviceLimit()) {
            // 消息队列超出限制，关闭连接
            log.info("Closing current session because msq queue size for device {} exceed limit {} with msgQueueSize counter {} and actual queue size {}",
                    deviceSessionCtx.getDeviceId(), context.getMessageQueueSizePerDeviceLimit(), queueSize, deviceSessionCtx.getMsgQueueSize());
            closeCtx(ctx, MqttReasonCodes.Disconnect.QUOTA_EXCEEDED);
            return;
        }
        // 添加到消息队列
        deviceSessionCtx.addToQueue(msg);
        // 尝试处理队列中的消息
        processMsgQueue(ctx); //Under the normal conditions the msg queue will contain 0 messages. Many messages will be processed on device connect event in separate thread pool
    }

    /**
     * 处理消息队列 - 如果设备已连接，则处理队列中积压的消息
     */
    void processMsgQueue(ChannelHandlerContext ctx) {
        if (!deviceSessionCtx.isConnected()) {
            log.trace("[{}][{}] Postpone processing msg due to device is not connected. Msg queue size is {}", sessionId, deviceSessionCtx.getDeviceId(), deviceSessionCtx.getMsgQueueSize());
            // 设备未连接，延迟处理
            return;
        }
        // 尝试处理队列中的消息（线程安全）
        deviceSessionCtx.tryProcessQueuedMsgs(msg -> processRegularSessionMsg(ctx, msg));
    }

    /**
     * 处理常规会话消息
     * @param ctx
     * @param msg
     */
    void processRegularSessionMsg(ChannelHandlerContext ctx, MqttMessage msg) {
        switch (msg.fixedHeader().messageType()) {
            case PUBLISH:
                // 处理发布消息
                processPublish(ctx, (MqttPublishMessage) msg);
                break;
            case SUBSCRIBE:
                // 处理订阅
                processSubscribe(ctx, (MqttSubscribeMessage) msg);
                break;
            case UNSUBSCRIBE:
                // 处理取消订阅
                processUnsubscribe(ctx, (MqttUnsubscribeMessage) msg);
                break;
            case PINGREQ:
                if (checkConnected(ctx, msg)) {
                    // 响应心跳
                    ctx.writeAndFlush(new MqttMessage(new MqttFixedHeader(PINGRESP, false, AT_MOST_ONCE, false, 0)));
                    // 记录设备活动
                    transportService.recordActivity(deviceSessionCtx.getSessionInfo());
                    if (gatewaySessionHandler != null) {
                        // 网关心跳处理
                        gatewaySessionHandler.onGatewayPing();
                    }
                }
                break;
            case DISCONNECT:
                // 处理客户端断开
                closeCtx(ctx, MqttReasonCodes.Disconnect.NORMAL_DISCONNECT);
                break;
            case PUBACK:
                // 处理发布确认，移除等待ACK的RPC请求
                int msgId = ((MqttPubAckMessage) msg).variableHeader().messageId();
                TransportProtos.ToDeviceRpcRequestMsg rpcRequest = rpcAwaitingAck.remove(msgId);
                if (rpcRequest != null) {
                    transportService.process(deviceSessionCtx.getSessionInfo(), rpcRequest, RpcStatus.DELIVERED, true, TransportServiceCallback.EMPTY);
                }
                break;
            default:
                break;
        }
    }

    /**
     * 处理PUBLISH消息 - 根据主题分发到不同处理器
     */
    private void processPublish(ChannelHandlerContext ctx, MqttPublishMessage mqttMsg) {
        // 检查连接状态
        if (!checkConnected(ctx, mqttMsg)) {
            return;
        }
        String topicName = mqttMsg.variableHeader().topicName();
        int msgId = mqttMsg.variableHeader().packetId();
        log.trace("[{}][{}] Processing publish msg [{}][{}]!", sessionId, deviceSessionCtx.getDeviceId(), topicName, msgId);
        if (topicName.startsWith(MqttTopics.BASE_GATEWAY_API_TOPIC)) {
            // 网关相关主题
            if (gatewaySessionHandler != null) {
                handleGatewayPublishMsg(ctx, topicName, msgId, mqttMsg);
                transportService.recordActivity(deviceSessionCtx.getSessionInfo());
            } else {
                log.error("[gatewaySessionHandler] is null, [{}] Failed to process publish msg [{}][{}]", sessionId, topicName, msgId);
            }
        } else if (sparkplugSessionHandler != null) {
            // Sparkplug协议相关主题
            handleSparkplugPublishMsg(ctx, topicName, mqttMsg);
        } else {
            // 普通设备主题
            processDevicePublish(ctx, mqttMsg, topicName, msgId);
        }
    }

    /**
     * 处理网关发布消息 - 根据具体主题调用网关处理器相应方法
     */
    private void handleGatewayPublishMsg(ChannelHandlerContext ctx, String topicName, int msgId, MqttPublishMessage mqttMsg) {
        try {
            switch (topicName) {
                case MqttTopics.GATEWAY_TELEMETRY_TOPIC:
                    // 设备遥测数据
                    gatewaySessionHandler.onDeviceTelemetry(mqttMsg);
                    break;
                case MqttTopics.GATEWAY_CLAIM_TOPIC:
                    // 设备认领
                    gatewaySessionHandler.onDeviceClaim(mqttMsg);
                    break;
                case MqttTopics.GATEWAY_ATTRIBUTES_TOPIC:
                    // 设备属性
                    gatewaySessionHandler.onDeviceAttributes(mqttMsg);
                    break;
                case MqttTopics.GATEWAY_ATTRIBUTES_REQUEST_TOPIC:
                    // 设备属性请求
                    gatewaySessionHandler.onDeviceAttributesRequest(mqttMsg);
                    break;
                case MqttTopics.GATEWAY_RPC_TOPIC:
                    // 设备RPC响应
                    gatewaySessionHandler.onDeviceRpcResponse(mqttMsg);
                    break;
                case MqttTopics.GATEWAY_CONNECT_TOPIC:
                    // 设备连接通知
                    gatewaySessionHandler.onDeviceConnect(mqttMsg);
                    break;
                case MqttTopics.GATEWAY_DISCONNECT_TOPIC:
                    // 设备断开通知
                    gatewaySessionHandler.onDeviceDisconnect(mqttMsg);
                    break;
                default:
                    // 不支持的网关主题
                    ack(ctx, msgId, MqttReasonCodes.PubAck.TOPIC_NAME_INVALID);
            }
        } catch (RuntimeException e) {
            log.warn("[{}] Failed to process publish msg [{}][{}]", sessionId, topicName, msgId, e);
            ack(ctx, msgId, MqttReasonCodes.PubAck.IMPLEMENTATION_SPECIFIC_ERROR);
            closeCtx(ctx, MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR);
        } catch (AdaptorException e) {
            log.debug("[{}] Failed to process publish msg [{}][{}]", sessionId, topicName, msgId, e);
            sendResponseForAdaptorErrorOrCloseContext(ctx, topicName, msgId);
        }
    }

    /**
     * 处理Sparkplug协议发布消息
     * Sparkplug是工业物联网专用协议，定义NBIRTH、DBIRTH等消息类型
     */
    private void handleSparkplugPublishMsg(ChannelHandlerContext ctx, String topicName, MqttPublishMessage mqttMsg) {
        int msgId = mqttMsg.variableHeader().packetId();
        try {
            // 解析Sparkplug主题
            SparkplugTopic sparkplugTopic = parseTopicPublish(topicName);
            if (sparkplugTopic.isNode()) {
                // A node topic
                // 节点主题（NBIRTH, NCMD, NDATA等）
                SparkplugBProto.Payload sparkplugBProtoNode = SparkplugBProto.Payload.parseFrom(ProtoMqttAdaptor.toBytes(mqttMsg.payload()));
                switch (sparkplugTopic.getType()) {
                    case NBIRTH:
                    case NCMD:
                    case NDATA:
                        sparkplugSessionHandler.onAttributesTelemetryProto(msgId, sparkplugBProtoNode, sparkplugTopic);
                        break;
                    default:
                }
            } else {
                // A device topic
                // 设备主题（DBIRTH, DCMD, DDATA, DDEATH等）
                SparkplugBProto.Payload sparkplugBProtoDevice = SparkplugBProto.Payload.parseFrom(ProtoMqttAdaptor.toBytes(mqttMsg.payload()));
                switch (sparkplugTopic.getType()) {
                    case DBIRTH:
                    case DCMD:
                    case DDATA:
                        sparkplugSessionHandler.onAttributesTelemetryProto(msgId, sparkplugBProtoDevice, sparkplugTopic);
                        break;
                    case DDEATH:
                        // 设备死亡通知
                        sparkplugSessionHandler.onDeviceDisconnect(mqttMsg, sparkplugTopic.getDeviceId());
                        break;
                    default:
                }
            }
        } catch (RuntimeException e) {
            log.error("[{}] Failed to process publish msg [{}][{}]", sessionId, topicName, msgId, e);
            ack(ctx, msgId, MqttReasonCodes.PubAck.IMPLEMENTATION_SPECIFIC_ERROR);
            closeCtx(ctx, MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR);
        } catch (AdaptorException | ThingsboardException | InvalidProtocolBufferException e) {
            log.error("[{}] Failed to process publish msg [{}][{}]", sessionId, topicName, msgId, e);
            sendResponseForAdaptorErrorOrCloseContext(ctx, topicName, msgId);
        }
    }

    /**
     * 处理普通设备发布消息 - 根据主题前缀分发到不同处理逻辑
     */
    private void processDevicePublish(ChannelHandlerContext ctx, MqttPublishMessage mqttMsg, String topicName, int msgId) {
        try {
            Matcher fwMatcher;
            MqttTransportAdaptor payloadAdaptor = deviceSessionCtx.getPayloadAdaptor();
            // 根据主题前缀处理不同类型的消息
            if (deviceSessionCtx.isDeviceAttributesTopic(topicName)) {
                // 设备属性上传
                TransportProtos.PostAttributeMsg postAttributeMsg = payloadAdaptor.convertToPostAttributes(deviceSessionCtx, mqttMsg);
                transportService.process(deviceSessionCtx.getSessionInfo(), postAttributeMsg, getMetadata(deviceSessionCtx, topicName),
                        getPubAckCallback(ctx, msgId, postAttributeMsg));
            } else if (deviceSessionCtx.isDeviceTelemetryTopic(topicName)) {
                // 设备遥测数据上传
                TransportProtos.PostTelemetryMsg postTelemetryMsg = payloadAdaptor.convertToPostTelemetry(deviceSessionCtx, mqttMsg);
                transportService.process(deviceSessionCtx.getSessionInfo(), postTelemetryMsg, getMetadata(deviceSessionCtx, topicName),
                        getPubAckCallback(ctx, msgId, postTelemetryMsg));
            } else if (topicName.startsWith(MqttTopics.DEVICE_ATTRIBUTES_REQUEST_TOPIC_PREFIX)) {
                // 设备属性请求
                TransportProtos.GetAttributeRequestMsg getAttributeMsg = payloadAdaptor.convertToGetAttributes(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_ATTRIBUTES_REQUEST_TOPIC_PREFIX);
                transportService.process(deviceSessionCtx.getSessionInfo(), getAttributeMsg, getPubAckCallback(ctx, msgId, getAttributeMsg));
                attrReqTopicType = TopicType.V1;
            } else if (topicName.startsWith(MqttTopics.DEVICE_RPC_RESPONSE_TOPIC)) {
                // 设备RPC响应
                TransportProtos.ToDeviceRpcResponseMsg rpcResponseMsg = payloadAdaptor.convertToDeviceRpcResponse(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_RPC_RESPONSE_TOPIC);
                transportService.process(deviceSessionCtx.getSessionInfo(), rpcResponseMsg, getPubAckCallback(ctx, msgId, rpcResponseMsg));
            } else if (topicName.startsWith(MqttTopics.DEVICE_RPC_REQUESTS_TOPIC)) {
                // 设备到服务器的RPC请求
                TransportProtos.ToServerRpcRequestMsg rpcRequestMsg = payloadAdaptor.convertToServerRpcRequest(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_RPC_REQUESTS_TOPIC);
                toServerRpcSubTopicType = TopicType.V1;
                if (SESSION_LIMITS.equals(rpcRequestMsg.getMethodName())) {
                    // 特殊RPC请求：获取会话限制
                    onGetSessionLimitsRpc(deviceSessionCtx.getSessionInfo(), ctx, msgId, rpcRequestMsg);
                } else {
                    transportService.process(deviceSessionCtx.getSessionInfo(), rpcRequestMsg, getPubAckCallback(ctx, msgId, rpcRequestMsg));
                }
            } else if (topicName.equals(MqttTopics.DEVICE_CLAIM_TOPIC)) {
                // 设备认领
                TransportProtos.ClaimDeviceMsg claimDeviceMsg = payloadAdaptor.convertToClaimDevice(deviceSessionCtx, mqttMsg);
                transportService.process(deviceSessionCtx.getSessionInfo(), claimDeviceMsg, getPubAckCallback(ctx, msgId, claimDeviceMsg));
            } else if ((fwMatcher = FW_REQUEST_PATTERN.matcher(topicName)).find()) {
                // 固件请求（OTA）
                getOtaPackageCallback(ctx, mqttMsg, msgId, fwMatcher, OtaPackageType.FIRMWARE);
            } else if ((fwMatcher = SW_REQUEST_PATTERN.matcher(topicName)).find()) {
                // 软件请求（OTA）
                getOtaPackageCallback(ctx, mqttMsg, msgId, fwMatcher, OtaPackageType.SOFTWARE);
            } else if (topicName.equals(MqttTopics.DEVICE_TELEMETRY_SHORT_TOPIC)) {
                // 短主题格式：遥测数据（旧版兼容）
                TransportProtos.PostTelemetryMsg postTelemetryMsg = payloadAdaptor.convertToPostTelemetry(deviceSessionCtx, mqttMsg);
                transportService.process(deviceSessionCtx.getSessionInfo(), postTelemetryMsg, getMetadata(deviceSessionCtx, topicName),
                        getPubAckCallback(ctx, msgId, postTelemetryMsg));
            } else if (topicName.equals(MqttTopics.DEVICE_TELEMETRY_SHORT_JSON_TOPIC)) {
                // 短主题格式：JSON遥测数据
                TransportProtos.PostTelemetryMsg postTelemetryMsg = context.getJsonMqttAdaptor().convertToPostTelemetry(deviceSessionCtx, mqttMsg);
                transportService.process(deviceSessionCtx.getSessionInfo(), postTelemetryMsg, getMetadata(deviceSessionCtx, topicName),
                        getPubAckCallback(ctx, msgId, postTelemetryMsg));
            } else if (topicName.equals(MqttTopics.DEVICE_TELEMETRY_SHORT_PROTO_TOPIC)) {
                // 短主题格式：Protobuf遥测数据
                TransportProtos.PostTelemetryMsg postTelemetryMsg = context.getProtoMqttAdaptor().convertToPostTelemetry(deviceSessionCtx, mqttMsg);
                transportService.process(deviceSessionCtx.getSessionInfo(), postTelemetryMsg, getMetadata(deviceSessionCtx, topicName),
                        getPubAckCallback(ctx, msgId, postTelemetryMsg));
            } else if (topicName.equals(MqttTopics.DEVICE_ATTRIBUTES_SHORT_TOPIC)) {
                // 短主题格式：属性上传
                TransportProtos.PostAttributeMsg postAttributeMsg = payloadAdaptor.convertToPostAttributes(deviceSessionCtx, mqttMsg);
                transportService.process(deviceSessionCtx.getSessionInfo(), postAttributeMsg, getMetadata(deviceSessionCtx, topicName),
                        getPubAckCallback(ctx, msgId, postAttributeMsg));
            } else if (topicName.equals(MqttTopics.DEVICE_ATTRIBUTES_SHORT_JSON_TOPIC)) {
                // 短主题格式：JSON属性上传
                TransportProtos.PostAttributeMsg postAttributeMsg = context.getJsonMqttAdaptor().convertToPostAttributes(deviceSessionCtx, mqttMsg);
                transportService.process(deviceSessionCtx.getSessionInfo(), postAttributeMsg, getMetadata(deviceSessionCtx, topicName),
                        getPubAckCallback(ctx, msgId, postAttributeMsg));
            } else if (topicName.equals(MqttTopics.DEVICE_ATTRIBUTES_SHORT_PROTO_TOPIC)) {
                // 短主题格式：Protobuf属性上传
                TransportProtos.PostAttributeMsg postAttributeMsg = context.getProtoMqttAdaptor().convertToPostAttributes(deviceSessionCtx, mqttMsg);
                transportService.process(deviceSessionCtx.getSessionInfo(), postAttributeMsg, getMetadata(deviceSessionCtx, topicName),
                        getPubAckCallback(ctx, msgId, postAttributeMsg));
            } else if (topicName.startsWith(MqttTopics.DEVICE_RPC_RESPONSE_SHORT_JSON_TOPIC)) {
                // 短主题格式：JSON RPC响应
                TransportProtos.ToDeviceRpcResponseMsg rpcResponseMsg = context.getJsonMqttAdaptor().convertToDeviceRpcResponse(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_RPC_RESPONSE_SHORT_JSON_TOPIC);
                transportService.process(deviceSessionCtx.getSessionInfo(), rpcResponseMsg, getPubAckCallback(ctx, msgId, rpcResponseMsg));
            } else if (topicName.startsWith(MqttTopics.DEVICE_RPC_RESPONSE_SHORT_PROTO_TOPIC)) {
                // 短主题格式：Protobuf RPC响应
                TransportProtos.ToDeviceRpcResponseMsg rpcResponseMsg = context.getProtoMqttAdaptor().convertToDeviceRpcResponse(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_RPC_RESPONSE_SHORT_PROTO_TOPIC);
                transportService.process(deviceSessionCtx.getSessionInfo(), rpcResponseMsg, getPubAckCallback(ctx, msgId, rpcResponseMsg));
            } else if (topicName.startsWith(MqttTopics.DEVICE_RPC_RESPONSE_SHORT_TOPIC)) {
                // 短主题格式：默认格式RPC响应
                TransportProtos.ToDeviceRpcResponseMsg rpcResponseMsg = payloadAdaptor.convertToDeviceRpcResponse(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_RPC_RESPONSE_SHORT_TOPIC);
                transportService.process(deviceSessionCtx.getSessionInfo(), rpcResponseMsg, getPubAckCallback(ctx, msgId, rpcResponseMsg));
            } else if (topicName.startsWith(MqttTopics.DEVICE_RPC_REQUESTS_SHORT_JSON_TOPIC)) {
                // 短主题格式：JSON RPC请求
                TransportProtos.ToServerRpcRequestMsg rpcRequestMsg = context.getJsonMqttAdaptor().convertToServerRpcRequest(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_RPC_REQUESTS_SHORT_JSON_TOPIC);
                transportService.process(deviceSessionCtx.getSessionInfo(), rpcRequestMsg, getPubAckCallback(ctx, msgId, rpcRequestMsg));
                toServerRpcSubTopicType = TopicType.V2_JSON;
            } else if (topicName.startsWith(MqttTopics.DEVICE_RPC_REQUESTS_SHORT_PROTO_TOPIC)) {
                // 短主题格式：Protobuf RPC请求
                TransportProtos.ToServerRpcRequestMsg rpcRequestMsg = context.getProtoMqttAdaptor().convertToServerRpcRequest(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_RPC_REQUESTS_SHORT_PROTO_TOPIC);
                transportService.process(deviceSessionCtx.getSessionInfo(), rpcRequestMsg, getPubAckCallback(ctx, msgId, rpcRequestMsg));
                toServerRpcSubTopicType = TopicType.V2_PROTO;
            } else if (topicName.startsWith(MqttTopics.DEVICE_RPC_REQUESTS_SHORT_TOPIC)) {
                // 短主题格式：默认格式RPC请求
                TransportProtos.ToServerRpcRequestMsg rpcRequestMsg = payloadAdaptor.convertToServerRpcRequest(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_RPC_REQUESTS_SHORT_TOPIC);
                transportService.process(deviceSessionCtx.getSessionInfo(), rpcRequestMsg, getPubAckCallback(ctx, msgId, rpcRequestMsg));
                toServerRpcSubTopicType = TopicType.V2;
            } else if (topicName.startsWith(MqttTopics.DEVICE_ATTRIBUTES_REQUEST_SHORT_JSON_TOPIC_PREFIX)) {
                // 短主题格式：JSON属性请求
                TransportProtos.GetAttributeRequestMsg getAttributeMsg = context.getJsonMqttAdaptor().convertToGetAttributes(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_ATTRIBUTES_REQUEST_SHORT_JSON_TOPIC_PREFIX);
                transportService.process(deviceSessionCtx.getSessionInfo(), getAttributeMsg, getPubAckCallback(ctx, msgId, getAttributeMsg));
                attrReqTopicType = TopicType.V2_JSON;
            } else if (topicName.startsWith(MqttTopics.DEVICE_ATTRIBUTES_REQUEST_SHORT_PROTO_TOPIC_PREFIX)) {
                // 短主题格式：Protobuf属性请求
                TransportProtos.GetAttributeRequestMsg getAttributeMsg = context.getProtoMqttAdaptor().convertToGetAttributes(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_ATTRIBUTES_REQUEST_SHORT_PROTO_TOPIC_PREFIX);
                transportService.process(deviceSessionCtx.getSessionInfo(), getAttributeMsg, getPubAckCallback(ctx, msgId, getAttributeMsg));
                attrReqTopicType = TopicType.V2_PROTO;
            } else if (topicName.startsWith(MqttTopics.DEVICE_ATTRIBUTES_REQUEST_SHORT_TOPIC_PREFIX)) {
                // 短主题格式：默认格式属性请求
                TransportProtos.GetAttributeRequestMsg getAttributeMsg = payloadAdaptor.convertToGetAttributes(deviceSessionCtx, mqttMsg, MqttTopics.DEVICE_ATTRIBUTES_REQUEST_SHORT_TOPIC_PREFIX);
                transportService.process(deviceSessionCtx.getSessionInfo(), getAttributeMsg, getPubAckCallback(ctx, msgId, getAttributeMsg));
                attrReqTopicType = TopicType.V2;
            } else {
                // 未知主题，记录活动并返回错误ACK
                transportService.recordActivity(deviceSessionCtx.getSessionInfo());
                ack(ctx, msgId, MqttReasonCodes.PubAck.TOPIC_NAME_INVALID);
            }
        } catch (AdaptorException e) {
            log.debug("[{}] Failed to process publish msg [{}][{}]", sessionId, topicName, msgId, e);
            sendResponseForAdaptorErrorOrCloseContext(ctx, topicName, msgId);
        }
    }


    /**
     * 获取元数据 - 如果是MQTT传输类型的设备，在元数据中添加MQTT主题信息
     */
    private TbMsgMetaData getMetadata(DeviceSessionCtx ctx, String topicName) {
        if (ctx.isDeviceProfileMqttTransportType()) {
            TbMsgMetaData md = new TbMsgMetaData();
            md.putValue(DataConstants.MQTT_TOPIC, topicName);
            return md;
        } else {
            return null;
        }
    }

    /**
     * 发送适配器错误响应或关闭连接
     * 根据MQTT版本和配置决定发送ACK还是直接关闭连接
     */
    private void sendResponseForAdaptorErrorOrCloseContext(ChannelHandlerContext ctx, String topicName, int msgId) {
        if ((deviceSessionCtx.isSendAckOnValidationException() || MqttVersion.MQTT_5.equals(deviceSessionCtx.getMqttVersion())) && msgId > 0) {
            log.debug("[{}] Send pub ack on invalid publish msg [{}][{}]", sessionId, topicName, msgId);
            ctx.writeAndFlush(createMqttPubAckMsg(deviceSessionCtx, msgId, MqttReasonCodes.PubAck.PAYLOAD_FORMAT_INVALID.byteValue()));
        } else {
            log.info("[{}] Closing current session due to invalid publish msg [{}][{}]", sessionId, topicName, msgId);
            closeCtx(ctx, MqttReasonCodes.Disconnect.PAYLOAD_FORMAT_INVALID);
        }
    }

    /**
     * 处理OTA包请求回调
     * OTA（Over-the-Air）固件/软件升级
     */
    private void getOtaPackageCallback(ChannelHandlerContext ctx, MqttPublishMessage mqttMsg, int msgId, Matcher fwMatcher, OtaPackageType type) {
        String payload = mqttMsg.content().toString(StandardCharsets.UTF_8);
        int chunkSize = StringUtils.isNotEmpty(payload) ? Integer.parseInt(payload) : 0;
        String requestId = fwMatcher.group("requestId");
        int chunk = Integer.parseInt(fwMatcher.group("chunk"));

        if (chunkSize > 0) {
            this.chunkSizes.put(requestId, chunkSize);
        } else {
            chunkSize = chunkSizes.getOrDefault(requestId, 0);
        }

        if (chunkSize > context.getMaxPayloadSize()) {
            sendOtaPackageError(ctx, PAYLOAD_TOO_LARGE);
            return;
        }

        String otaPackageId = otaPackSessions.get(requestId);

        if (otaPackageId != null) {
            // 已缓存OTA包ID，直接发送包数据
            sendOtaPackage(ctx, mqttMsg.variableHeader().packetId(), otaPackageId, requestId, chunkSize, chunk, type);
        } else {
            // 首次请求，向服务请求OTA包信息
            TransportProtos.SessionInfoProto sessionInfo = deviceSessionCtx.getSessionInfo();
            TransportProtos.GetOtaPackageRequestMsg getOtaPackageRequestMsg = TransportProtos.GetOtaPackageRequestMsg.newBuilder()
                    .setDeviceIdMSB(sessionInfo.getDeviceIdMSB())
                    .setDeviceIdLSB(sessionInfo.getDeviceIdLSB())
                    .setTenantIdMSB(sessionInfo.getTenantIdMSB())
                    .setTenantIdLSB(sessionInfo.getTenantIdLSB())
                    .setType(type.name())
                    .build();
            transportService.process(deviceSessionCtx.getSessionInfo(), getOtaPackageRequestMsg,
                    new OtaPackageCallback(ctx, msgId, getOtaPackageRequestMsg, requestId, chunkSize, chunk));
        }
    }

    /**
     * 发送ACK响应
     */
    private void ack(ChannelHandlerContext ctx, int msgId, MqttReasonCodes.PubAck returnCode) {
        ack(ctx, msgId, returnCode.byteValue());
    }

    private void ack(ChannelHandlerContext ctx, int msgId, byte returnCode) {
        if (msgId > 0) {
            ctx.writeAndFlush(createMqttPubAckMsg(deviceSessionCtx, msgId, returnCode));
        }
    }

    /**
     * 创建发布ACK回调
     * 用于处理消息发布后的成功/失败逻辑
     */
    private <T> TransportServiceCallback<Void> getPubAckCallback(final ChannelHandlerContext ctx, final int msgId, final T msg) {
        return new TransportServiceCallback<>() {
            @Override
            public void onSuccess(Void dummy) {
                log.trace("[{}] Published msg: {}", sessionId, msg);
                ack(ctx, msgId, MqttReasonCodes.PubAck.SUCCESS);
            }

            @Override
            public void onError(Throwable e) {
                log.trace("[{}] Failed to publish msg: {}", sessionId, msg, e);
                closeCtx(ctx, MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR);
            }
        };
    }

    /**
     * 设备预配置回调类
     * 处理设备预配置请求的响应
     */
    private class DeviceProvisionCallback implements TransportServiceCallback<ProvisionDeviceResponseMsg> {
        private final ChannelHandlerContext ctx;
        private final int msgId;
        private final TransportProtos.ProvisionDeviceRequestMsg msg;

        DeviceProvisionCallback(ChannelHandlerContext ctx, int msgId, TransportProtos.ProvisionDeviceRequestMsg msg) {
            this.ctx = ctx;
            this.msgId = msgId;
            this.msg = msg;
        }

        @Override
        public void onSuccess(TransportProtos.ProvisionDeviceResponseMsg provisionResponseMsg) {
            log.trace("[{}] Published msg: {}", sessionId, msg);
            ack(ctx, msgId, MqttReasonCodes.PubAck.SUCCESS);
            try {
                // 根据配置的负载类型发送预配置响应
                if (deviceSessionCtx.getProvisionPayloadType().equals(TransportPayloadType.JSON)) {
                    deviceSessionCtx.getContext().getJsonMqttAdaptor().convertToPublish(deviceSessionCtx, provisionResponseMsg).ifPresent(deviceSessionCtx.getChannel()::writeAndFlush);
                } else {
                    deviceSessionCtx.getContext().getProtoMqttAdaptor().convertToPublish(deviceSessionCtx, provisionResponseMsg).ifPresent(deviceSessionCtx.getChannel()::writeAndFlush);
                }
                // 预配置完成后60秒关闭连接
                scheduler.schedule((Callable<ChannelFuture>) ctx::close, 60, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.trace("[{}] Failed to convert device provision response to MQTT msg", sessionId, e);
            }
        }

        @Override
        public void onError(Throwable e) {
            log.trace("[{}] Failed to publish msg: {}", sessionId, msg, e);
            ack(ctx, msgId, MqttReasonCodes.PubAck.IMPLEMENTATION_SPECIFIC_ERROR);
            closeCtx(ctx, MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR);
        }
    }

    /**
     * OTA包回调类
     * 处理OTA包请求的响应
     */
    private class OtaPackageCallback implements TransportServiceCallback<TransportProtos.GetOtaPackageResponseMsg> {
        private final ChannelHandlerContext ctx;
        private final int msgId;
        private final TransportProtos.GetOtaPackageRequestMsg msg;
        private final String requestId;
        private final int chunkSize;
        private final int chunk;

        OtaPackageCallback(ChannelHandlerContext ctx, int msgId, TransportProtos.GetOtaPackageRequestMsg msg, String requestId, int chunkSize, int chunk) {
            this.ctx = ctx;
            this.msgId = msgId;
            this.msg = msg;
            this.requestId = requestId;
            this.chunkSize = chunkSize;
            this.chunk = chunk;
        }

        @Override
        public void onSuccess(TransportProtos.GetOtaPackageResponseMsg response) {
            if (TransportProtos.ResponseStatus.SUCCESS.equals(response.getResponseStatus())) {
                // 成功获取OTA包，缓存包ID并发送包数据
                OtaPackageId firmwareId = new OtaPackageId(new UUID(response.getOtaPackageIdMSB(), response.getOtaPackageIdLSB()));
                otaPackSessions.put(requestId, firmwareId.toString());
                sendOtaPackage(ctx, msgId, firmwareId.toString(), requestId, chunkSize, chunk, OtaPackageType.valueOf(response.getType()));
            } else {
                sendOtaPackageError(ctx, response.getResponseStatus().toString());
            }
        }

        @Override
        public void onError(Throwable e) {
            log.trace("[{}] Failed to get firmware: {}", sessionId, msg, e);
            closeCtx(ctx, MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR);
        }
    }

    /**
     * 发送OTA包数据到设备
     */
    private void sendOtaPackage(ChannelHandlerContext ctx, int msgId, String firmwareId, String requestId, int chunkSize, int chunk, OtaPackageType type) {
        log.trace("[{}] Send firmware [{}] to device!", sessionId, firmwareId);
        ack(ctx, msgId, MqttReasonCodes.PubAck.SUCCESS);
        try {
            // 从缓存获取OTA包数据块
            byte[] firmwareChunk = context.getOtaPackageDataCache().get(firmwareId, chunkSize, chunk);
            deviceSessionCtx.getPayloadAdaptor()
                    .convertToPublish(deviceSessionCtx, firmwareChunk, requestId, chunk, type)
                    .ifPresent(deviceSessionCtx.getChannel()::writeAndFlush);
        } catch (Exception e) {
            log.trace("[{}] Failed to send firmware response!", sessionId, e);
        }
    }

    /**
     * 发送OTA包错误
     */
    private void sendOtaPackageError(ChannelHandlerContext ctx, String error) {
        log.warn("[{}] {}", sessionId, error);
        deviceSessionCtx.getChannel().writeAndFlush(deviceSessionCtx
                .getPayloadAdaptor()
                .createMqttPublishMsg(deviceSessionCtx, MqttTopics.DEVICE_FIRMWARE_ERROR_TOPIC, error.getBytes()));
        closeCtx(ctx, MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR);
    }

    /**
     * 处理SUBSCRIBE消息
     * 设备订阅主题请求
     */
    private void processSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage mqttMsg) {
        if (!checkConnected(ctx, mqttMsg) && !deviceSessionCtx.isProvisionOnly()) {
            // 未连接且不是预配置会话，返回未授权
            ctx.writeAndFlush(createSubAckMessage(mqttMsg.variableHeader().messageId(), Collections.singletonList(MqttReasonCodes.SubAck.NOT_AUTHORIZED.byteValue() & 0xFF)));
            return;
        }
        //TODO consume the rate limit
        log.trace("[{}][{}] Processing subscription [{}]!", deviceSessionCtx.getTenantId(), sessionId, mqttMsg.variableHeader().messageId());
        List<Integer> grantedQoSList = new ArrayList<>();
        boolean activityReported = false;
        // 遍历所有订阅请求
        for (MqttTopicSubscription subscription : mqttMsg.payload().topicSubscriptions()) {
            String topic = subscription.topicName();
            MqttQoS reqQoS = subscription.qualityOfService();
            if (deviceSessionCtx.isProvisionOnly()) {
                // 预配置会话只允许订阅预配置响应主题
                if (MqttTopics.DEVICE_PROVISION_RESPONSE_TOPIC.equals(topic)) {
                    registerSubQoS(topic, grantedQoSList, reqQoS);
                } else {
                    log.debug("[{}][{}] Failed to subscribe because this session is provision only [{}][{}]", deviceSessionCtx.getTenantId(), sessionId, topic, reqQoS);
                    grantedQoSList.add(ReturnCodeResolver.getSubscriptionReturnCode(deviceSessionCtx.getMqttVersion(), MqttReasonCodes.SubAck.TOPIC_FILTER_INVALID));
                }
                activityReported = true;
                continue;
            }
            if (deviceSessionCtx.isDeviceSubscriptionAttributesTopic(topic)) {
                // 设备属性订阅主题（v1格式）
                processAttributesSubscribe(grantedQoSList, topic, reqQoS, TopicType.V1);
                activityReported = true;
                continue;
            }
            try {
                if (sparkplugSessionHandler != null) {
                    // Sparkplug设备订阅处理
                    sparkplugSessionHandler.handleSparkplugSubscribeMsg(grantedQoSList, subscription, reqQoS);
                    activityReported = true;
                } else {
                    // 普通设备订阅处理
                    switch (topic) {
                        case MqttTopics.DEVICE_ATTRIBUTES_TOPIC: {
                            processAttributesSubscribe(grantedQoSList, topic, reqQoS, TopicType.V1);
                            activityReported = true;
                            break;
                        }
                        case MqttTopics.DEVICE_ATTRIBUTES_SHORT_TOPIC: {
                            processAttributesSubscribe(grantedQoSList, topic, reqQoS, TopicType.V2);
                            activityReported = true;
                            break;
                        }
                        case MqttTopics.DEVICE_ATTRIBUTES_SHORT_JSON_TOPIC: {
                            processAttributesSubscribe(grantedQoSList, topic, reqQoS, TopicType.V2_JSON);
                            activityReported = true;
                            break;
                        }
                        case MqttTopics.DEVICE_ATTRIBUTES_SHORT_PROTO_TOPIC: {
                            processAttributesSubscribe(grantedQoSList, topic, reqQoS, TopicType.V2_PROTO);
                            activityReported = true;
                            break;
                        }
                        case MqttTopics.DEVICE_RPC_REQUESTS_SUB_TOPIC: {
                            processRpcSubscribe(grantedQoSList, topic, reqQoS, TopicType.V1);
                            activityReported = true;
                            break;
                        }
                        case MqttTopics.DEVICE_RPC_REQUESTS_SUB_SHORT_TOPIC: {
                            processRpcSubscribe(grantedQoSList, topic, reqQoS, TopicType.V2);
                            activityReported = true;
                            break;
                        }
                        case MqttTopics.DEVICE_RPC_REQUESTS_SUB_SHORT_JSON_TOPIC: {
                            processRpcSubscribe(grantedQoSList, topic, reqQoS, TopicType.V2_JSON);
                            activityReported = true;
                            break;
                        }
                        case MqttTopics.DEVICE_RPC_REQUESTS_SUB_SHORT_PROTO_TOPIC: {
                            processRpcSubscribe(grantedQoSList, topic, reqQoS, TopicType.V2_PROTO);
                            activityReported = true;
                            break;
                        }
                        case MqttTopics.DEVICE_RPC_RESPONSE_SUB_TOPIC:
                        case MqttTopics.DEVICE_RPC_RESPONSE_SUB_SHORT_TOPIC:
                        case MqttTopics.DEVICE_RPC_RESPONSE_SUB_SHORT_JSON_TOPIC:
                        case MqttTopics.DEVICE_RPC_RESPONSE_SUB_SHORT_PROTO_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_RESPONSES_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_RESPONSES_SHORT_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_RESPONSES_SHORT_JSON_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_RESPONSES_SHORT_PROTO_TOPIC:
                        case MqttTopics.GATEWAY_ATTRIBUTES_TOPIC:
                        case MqttTopics.GATEWAY_RPC_TOPIC:
                        case MqttTopics.GATEWAY_ATTRIBUTES_RESPONSE_TOPIC:
                        case MqttTopics.DEVICE_FIRMWARE_RESPONSES_TOPIC:
                        case MqttTopics.DEVICE_FIRMWARE_ERROR_TOPIC:
                        case MqttTopics.DEVICE_SOFTWARE_RESPONSES_TOPIC:
                        case MqttTopics.DEVICE_SOFTWARE_ERROR_TOPIC:
                            // 只订阅不注册特殊处理的响应主题
                            registerSubQoS(topic, grantedQoSList, reqQoS);
                            break;
                        default:
                            // 不支持的订阅主题
                            //TODO increment an error counter if any exists
                            log.warn("[{}][{}] Failed to subscribe because topic is not supported [{}][{}]", deviceSessionCtx.getTenantId(), sessionId, topic, reqQoS);
                            grantedQoSList.add(ReturnCodeResolver.getSubscriptionReturnCode(deviceSessionCtx.getMqttVersion(), MqttReasonCodes.SubAck.TOPIC_FILTER_INVALID));
                            break;
                    }
                }
            } catch (Exception e) {
                log.warn("[{}][{}] Failed to subscribe to [{}][{}]", deviceSessionCtx.getTenantId(), sessionId, topic, reqQoS, e);
                grantedQoSList.add(ReturnCodeResolver.getSubscriptionReturnCode(deviceSessionCtx.getMqttVersion(), MqttReasonCodes.SubAck.IMPLEMENTATION_SPECIFIC_ERROR));
            }
        }
        if (!activityReported) {
            transportService.recordActivity(deviceSessionCtx.getSessionInfo());
        }
        ctx.writeAndFlush(createSubAckMessage(mqttMsg.variableHeader().messageId(), grantedQoSList));
    }

    /**
     * 处理RPC订阅 - 注册到RPC订阅服务
     */
    private void processRpcSubscribe(List<Integer> grantedQoSList, String topic, MqttQoS reqQoS, TopicType topicType) {
        transportService.process(deviceSessionCtx.getSessionInfo(), TransportProtos.SubscribeToRPCMsg.newBuilder().build(), null);
        rpcSubTopicType = topicType;
        registerSubQoS(topic, grantedQoSList, reqQoS);
    }

    /**
     * 处理属性订阅 - 注册到属性更新订阅服务
     */
    private void processAttributesSubscribe(List<Integer> grantedQoSList, String topic, MqttQoS reqQoS, TopicType topicType) {
        transportService.process(deviceSessionCtx.getSessionInfo(), TransportProtos.SubscribeToAttributeUpdatesMsg.newBuilder().build(), null);
        attrSubTopicType = topicType;
        registerSubQoS(topic, grantedQoSList, reqQoS);
    }

    /**
     * Sparkplug节点同时订阅属性和RPC
     */
    public void processAttributesRpcSubscribeSparkplugNode(List<Integer> grantedQoSList, MqttQoS reqQoS) {
        transportService.process(TransportProtos.TransportToDeviceActorMsg.newBuilder()
                .setSessionInfo(deviceSessionCtx.getSessionInfo())
                .setSubscribeToAttributes(SUBSCRIBE_TO_ATTRIBUTE_UPDATES_ASYNC_MSG)
                .setSubscribeToRPC(SUBSCRIBE_TO_RPC_ASYNC_MSG)
                .build(), null);
        registerSubQoS(MqttTopics.DEVICE_ATTRIBUTES_TOPIC, grantedQoSList, reqQoS);
    }

    /**
     * 注册订阅QoS - 将主题和对应的QoS存储到映射中
     */
    public void registerSubQoS(String topic, List<Integer> grantedQoSList, MqttQoS reqQoS) {
        grantedQoSList.add(getMinSupportedQos(reqQoS));
        mqttQoSMap.put(new MqttTopicMatcher(topic), getMinSupportedQos(reqQoS));
    }

    /**
     * 处理UNSUBSCRIBE消息 - 取消订阅
     */
    private void processUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage mqttMsg) {
        if (!checkConnected(ctx, mqttMsg) && !deviceSessionCtx.isProvisionOnly()) {
            ctx.writeAndFlush(createUnSubAckMessage(mqttMsg.variableHeader().messageId(),
                    Collections.singletonList((short) MqttReasonCodes.UnsubAck.NOT_AUTHORIZED.byteValue())));
            return;
        }
        boolean activityReported = false;
        List<Short> unSubResults = new ArrayList<>();
        log.trace("[{}] Processing subscription [{}]!", sessionId, mqttMsg.variableHeader().messageId());
        // 遍历所有要取消的订阅
        for (String topicName : mqttMsg.payload().topics()) {
            MqttTopicMatcher matcher = new MqttTopicMatcher(topicName);
            if (mqttQoSMap.containsKey(matcher)) {
                // 从QoS映射中移除
                mqttQoSMap.remove(matcher);
                try {
                    short resultValue = MqttReasonCodes.UnsubAck.SUCCESS.byteValue();
                    if (deviceSessionCtx.isProvisionOnly()) {
                        if (!matcher.matches(MqttTopics.DEVICE_PROVISION_RESPONSE_TOPIC)) {
                            resultValue = MqttReasonCodes.UnsubAck.TOPIC_FILTER_INVALID.byteValue();
                        }
                        unSubResults.add(resultValue);
                        activityReported = true;
                        continue;
                    }
                    switch (topicName) {
                        case MqttTopics.DEVICE_ATTRIBUTES_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_SHORT_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_SHORT_PROTO_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_SHORT_JSON_TOPIC: {
                            // 取消属性订阅
                            transportService.process(deviceSessionCtx.getSessionInfo(),
                                    TransportProtos.SubscribeToAttributeUpdatesMsg.newBuilder().setUnsubscribe(true).build(), null);
                            activityReported = true;
                            break;
                        }
                        case MqttTopics.DEVICE_RPC_REQUESTS_SUB_TOPIC:
                        case MqttTopics.DEVICE_RPC_REQUESTS_SUB_SHORT_TOPIC:
                        case MqttTopics.DEVICE_RPC_REQUESTS_SUB_SHORT_JSON_TOPIC:
                        case MqttTopics.DEVICE_RPC_REQUESTS_SUB_SHORT_PROTO_TOPIC: {
                            // 取消RPC订阅
                            transportService.process(deviceSessionCtx.getSessionInfo(),
                                    TransportProtos.SubscribeToRPCMsg.newBuilder().setUnsubscribe(true).build(), null);
                            activityReported = true;
                            break;
                        }
                        case MqttTopics.DEVICE_RPC_RESPONSE_SUB_TOPIC:
                        case MqttTopics.DEVICE_RPC_RESPONSE_SUB_SHORT_TOPIC:
                        case MqttTopics.DEVICE_RPC_RESPONSE_SUB_SHORT_JSON_TOPIC:
                        case MqttTopics.DEVICE_RPC_RESPONSE_SUB_SHORT_PROTO_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_RESPONSES_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_RESPONSES_SHORT_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_RESPONSES_SHORT_JSON_TOPIC:
                        case MqttTopics.DEVICE_ATTRIBUTES_RESPONSES_SHORT_PROTO_TOPIC:
                        case MqttTopics.GATEWAY_ATTRIBUTES_TOPIC:
                        case MqttTopics.GATEWAY_RPC_TOPIC:
                        case MqttTopics.GATEWAY_ATTRIBUTES_RESPONSE_TOPIC:
                        case MqttTopics.DEVICE_FIRMWARE_RESPONSES_TOPIC:
                        case MqttTopics.DEVICE_FIRMWARE_ERROR_TOPIC:
                        case MqttTopics.DEVICE_SOFTWARE_RESPONSES_TOPIC:
                        case MqttTopics.DEVICE_SOFTWARE_ERROR_TOPIC: {
                            // 响应主题取消订阅，无需特殊处理
                            activityReported = true;
                            break;
                        }
                        default:
                            log.trace("[{}] Failed to process unsubscription [{}] to [{}]", sessionId, mqttMsg.variableHeader().messageId(), topicName);
                            resultValue = MqttReasonCodes.UnsubAck.TOPIC_FILTER_INVALID.byteValue();
                    }
                    unSubResults.add(resultValue);
                } catch (Exception e) {
                    log.debug("[{}] Failed to process unsubscription [{}] to [{}]", sessionId, mqttMsg.variableHeader().messageId(), topicName);
                    unSubResults.add((short) MqttReasonCodes.UnsubAck.IMPLEMENTATION_SPECIFIC_ERROR.byteValue());
                }
            } else {
                log.debug("[{}] Failed to process unsubscription [{}] to [{}] - Subscription not found", sessionId, mqttMsg.variableHeader().messageId(), topicName);
                unSubResults.add((short) MqttReasonCodes.UnsubAck.NO_SUBSCRIPTION_EXISTED.byteValue());
            }
        }
        if (!activityReported && !deviceSessionCtx.isProvisionOnly()) {
            transportService.recordActivity(deviceSessionCtx.getSessionInfo());
        }
        ctx.writeAndFlush(createUnSubAckMessage(mqttMsg.variableHeader().messageId(), unSubResults));
    }

    /**
     * 创建取消订阅确认消息
     */
    private MqttMessage createUnSubAckMessage(int msgId, List<Short> resultCodes) {
        MqttMessageBuilders.UnsubAckBuilder unsubAckBuilder = MqttMessageBuilders.unsubAck();
        unsubAckBuilder.packetId(msgId);
        if (MqttVersion.MQTT_5.equals(deviceSessionCtx.getMqttVersion())) {
            unsubAckBuilder.addReasonCodes(resultCodes.toArray(Short[]::new));
        }
        return unsubAckBuilder.build();
    }

    /**
     * 处理CONNECT消息 - MQTT连接建立
     * 这是MQTT协议的第一步，客户端发起连接请求
     */
    void processConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {
        log.debug("[{}][{}] Processing connect msg for client: {}!", address, sessionId, msg.payload().clientIdentifier());
        String userName = msg.payload().userName();
        String clientId = msg.payload().clientIdentifier();
        deviceSessionCtx.setMqttVersion(getMqttVersion(msg.variableHeader().version()));
        // 检查是否为预配置连接（userName或clientId为"provision"）
        if (DataConstants.PROVISION.equals(userName) || DataConstants.PROVISION.equals(clientId)) {
            deviceSessionCtx.setProvisionOnly(true);
            ctx.writeAndFlush(createMqttConnAckMsg(MqttConnectReturnCode.CONNECTION_ACCEPTED, msg));
        } else {
            // 检查是否使用X509证书认证
            X509Certificate cert;
            if (sslHandler != null && (cert = getX509Certificate()) != null) {
                // X509证书认证
                processX509CertConnect(ctx, cert, msg);
            } else {
                // 用户名/密码认证
                processAuthTokenConnect(ctx, msg);
            }
        }
    }

    /**
     * 处理用户名/密码认证连接
     */
    private void processAuthTokenConnect(ChannelHandlerContext ctx, MqttConnectMessage connectMessage) {
        String userName = connectMessage.payload().userName();
        log.debug("[{}][{}] Processing connect msg for client with user name: {}!", address, sessionId, userName);
        TransportProtos.ValidateBasicMqttCredRequestMsg.Builder request = TransportProtos.ValidateBasicMqttCredRequestMsg.newBuilder()
                .setClientId(connectMessage.payload().clientIdentifier());
        if (userName != null) {
            request.setUserName(userName);
        }
        byte[] passwordBytes = connectMessage.payload().passwordInBytes();
        if (passwordBytes != null) {
            String password = new String(passwordBytes, CharsetUtil.UTF_8);
            request.setPassword(password);
        }
        // 调用传输服务验证凭证
        transportService.process(DeviceTransportType.MQTT, request.build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                        onValidateDeviceResponse(msg, ctx, connectMessage);
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.trace("[{}] Failed to process credentials: {}", address, userName, e);
                        ctx.writeAndFlush(createMqttConnAckMsg(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE_5, connectMessage));
                        closeCtx(ctx, MqttReasonCodes.Disconnect.SERVER_BUSY);
                    }
                });
    }

    /**
     * 处理X509证书认证连接
     */
    private void processX509CertConnect(ChannelHandlerContext ctx, X509Certificate cert, MqttConnectMessage connectMessage) {
        try {
            if (!context.isSkipValidityCheckForClientCert()) {
                // 检查证书有效期
                cert.checkValidity();
            }
            String strCert = SslUtil.getCertificateString(cert);
            // 计算证书哈希
            String sha3Hash = EncryptionUtil.getSha3Hash(strCert);
            transportService.process(DeviceTransportType.MQTT, ValidateDeviceX509CertRequestMsg.newBuilder().setHash(sha3Hash).build(),
                    new TransportServiceCallback<>() {
                        @Override
                        public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                            onValidateDeviceResponse(msg, ctx, connectMessage);
                        }

                        @Override
                        public void onError(Throwable e) {
                            log.trace("[{}] Failed to process credentials: {}", address, sha3Hash, e);
                            ctx.writeAndFlush(createMqttConnAckMsg(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE_5, connectMessage));
                            closeCtx(ctx, MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR);
                        }
                    });
        } catch (Exception e) {
            // 记录认证失败
            context.onAuthFailure(address);
            ctx.writeAndFlush(createMqttConnAckMsg(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED_5, connectMessage));
            log.trace("[{}] X509 auth failure: {}", sessionId, address, e);
            closeCtx(ctx, MqttReasonCodes.Disconnect.NOT_AUTHORIZED);
        }
    }

    /**
     * 从SSL会话中获取X509证书
     */
    private X509Certificate getX509Certificate() {
        try {
            Certificate[] certChain = sslHandler.engine().getSession().getPeerCertificates();
            if (certChain.length > 0) {
                return (X509Certificate) certChain[0];
            }
        } catch (SSLPeerUnverifiedException e) {
            log.warn(e.getMessage());
            return null;
        }
        return null;
    }

    /**
     * 创建MQTT连接确认消息
     */
    private MqttConnAckMessage createMqttConnAckMsg(MqttConnectReturnCode returnCode, MqttConnectMessage msg) {
        MqttMessageBuilders.ConnAckBuilder connAckBuilder = MqttMessageBuilders.connAck();
        connAckBuilder.sessionPresent(!msg.variableHeader().isCleanSession());
        MqttConnectReturnCode finalReturnCode = ReturnCodeResolver.getConnectionReturnCode(deviceSessionCtx.getMqttVersion(), returnCode);
        connAckBuilder.returnCode(finalReturnCode);
        return connAckBuilder.build();
    }

    /**
     * 刷新输出缓冲区
     * @param ctx
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    /**
     * 异常捕获 - 处理通道异常
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof IOException) {
            // 网络IO异常，通常为客户端断开连接
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}][{}] IOException: {}", sessionId,
                        Optional.ofNullable(this.deviceSessionCtx.getDeviceInfo()).map(TransportDeviceInfo::getDeviceId).orElse(null),
                        Optional.ofNullable(this.deviceSessionCtx.getDeviceInfo()).map(TransportDeviceInfo::getDeviceName).orElse(""),
                        cause.getMessage(),
                        cause);
            } else if (log.isInfoEnabled()) {
                log.info("[{}][{}][{}] IOException: {}", sessionId,
                        Optional.ofNullable(this.deviceSessionCtx.getDeviceInfo()).map(TransportDeviceInfo::getDeviceId).orElse(null),
                        Optional.ofNullable(this.deviceSessionCtx.getDeviceInfo()).map(TransportDeviceInfo::getDeviceName).orElse(""),
                        cause.getMessage());
            }
        } else {
            log.error("[{}] Unexpected Exception", sessionId, cause);
        }

        closeCtx(ctx, MqttReasonCodes.Disconnect.SERVER_SHUTTING_DOWN);
        if (cause instanceof OutOfMemoryError) {
            // 内存溢出，退出进程
            log.error("Received critical error. Going to shutdown the service.");
            System.exit(1);
        }
    }

    /**
     * 创建订阅确认消息
     */
    private static MqttSubAckMessage createSubAckMessage(Integer msgId, List<Integer> reasonCodes) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(SUBACK, false, AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(msgId);
        MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(reasonCodes);
        return new MqttSubAckMessage(mqttFixedHeader, mqttMessageIdVariableHeader, mqttSubAckPayload);
    }

    /**
     * 获取最小支持的QoS等级（不超过系统支持的最大等级）
     */
    private static int getMinSupportedQos(MqttQoS reqQoS) {
        return Math.min(reqQoS.value(), MAX_SUPPORTED_QOS_LVL.value());
    }

    /**
     * 根据版本号获取MQTT版本枚举
     */
    private static MqttVersion getMqttVersion(int versionCode) {
        switch (versionCode) {
            case 3:
                return MqttVersion.MQTT_3_1;
            case 5:
                return MqttVersion.MQTT_5;
            default:
                return MqttVersion.MQTT_3_1_1;
        }
    }

    /**
     * 创建MQTT发布确认消息
     */
    public static MqttMessage createMqttPubAckMsg(DeviceSessionCtx deviceSessionCtx, int requestId, byte returnCode) {
        MqttMessageBuilders.PubAckBuilder pubAckMsgBuilder = MqttMessageBuilders.pubAck().packetId(requestId);
        if (MqttVersion.MQTT_5.equals(deviceSessionCtx.getMqttVersion())) {
            pubAckMsgBuilder.reasonCode(returnCode);
        }
        return pubAckMsgBuilder.build();
    }

    /**
     * 创建MQTT断开连接消息
     */
    public static MqttMessage createMqttDisconnectMsg(DeviceSessionCtx deviceSessionCtx, byte returnCode) {
        MqttMessageBuilders.DisconnectBuilder disconnectBuilder = MqttMessageBuilders.disconnect();
        if (MqttVersion.MQTT_5.equals(deviceSessionCtx.getMqttVersion())) {
            disconnectBuilder.reasonCode(returnCode);
        }
        return disconnectBuilder.build();
    }

    /**
     * 检查连接状态 - 如果未连接则关闭会话
     */
    private boolean checkConnected(ChannelHandlerContext ctx, MqttMessage msg) {
        if (deviceSessionCtx.isConnected()) {
            return true;
        } else {
            log.info("[{}] Closing current session due to invalid msg order: {}", sessionId, msg);
            return false;
        }
    }

    /**
     * 检查是否为网关会话 - 从设备附加信息中解析网关配置
     */
    private void checkGatewaySession(SessionMetaData sessionMetaData) {
        TransportDeviceInfo device = deviceSessionCtx.getDeviceInfo();
        try {
            JsonNode infoNode = context.getMapper().readTree(device.getAdditionalInfo());
            if (infoNode != null) {
                JsonNode gatewayNode = infoNode.get(DataConstants.GATEWAY_PARAMETER);
                if (gatewayNode != null && gatewayNode.asBoolean()) {
                    // 设备是网关，创建网关会话处理器
                    boolean overwriteDevicesActivity = false;
                    if (infoNode.has(DataConstants.OVERWRITE_ACTIVITY_TIME_PARAMETER)
                            && infoNode.get(DataConstants.OVERWRITE_ACTIVITY_TIME_PARAMETER).isBoolean()) {
                        overwriteDevicesActivity = infoNode.get(DataConstants.OVERWRITE_ACTIVITY_TIME_PARAMETER).asBoolean();
                        sessionMetaData.setOverwriteActivityTime(overwriteDevicesActivity);
                    }
                    gatewaySessionHandler = new GatewaySessionHandler(deviceSessionCtx, sessionId, overwriteDevicesActivity);
                }
            }
        } catch (IOException e) {
            log.trace("[{}][{}] Failed to fetch device additional info", sessionId, device.getDeviceName(), e);
        }
    }

    /**
     * 检查是否为Sparkplug节点会话 - 解析Will消息判断
     */
    private void checkSparkplugNodeSession(MqttConnectMessage connectMessage, ChannelHandlerContext ctx, SessionMetaData sessionMetaData) {
        try {
            if (sparkplugSessionHandler == null) {
                SparkplugTopic sparkplugTopicNode = validatedSparkplugTopicConnectedNode(connectMessage);
                if (sparkplugTopicNode != null) {
                    // 解析Sparkplug协议Will消息（NDEATH）
                    SparkplugBProto.Payload sparkplugBProtoNode = SparkplugBProto.Payload.parseFrom(connectMessage.payload().willMessageInBytes());
                    sparkplugSessionHandler = new SparkplugNodeSessionHandler(this, deviceSessionCtx, sessionId, true, sparkplugTopicNode);
                    sparkplugSessionHandler.onAttributesTelemetryProto(0, sparkplugBProtoNode, sparkplugTopicNode);
                    sessionMetaData.setOverwriteActivityTime(true);
                } else {
                    log.trace("[{}][{}] Failed to fetch sparkplugDevice connect:  sparkplugTopicName without SparkplugMessageType.NDEATH.", sessionId, deviceSessionCtx.getDeviceInfo().getDeviceName());
                    throw new ThingsboardException("Invalid request body", ThingsboardErrorCode.BAD_REQUEST_PARAMS);
                }
            }
        } catch (Exception e) {
            log.trace("[{}][{}] Failed to fetch sparkplugDevice connect, sparkplugTopicName", sessionId, deviceSessionCtx.getDeviceInfo().getDeviceName(), e);
            ctx.writeAndFlush(createMqttConnAckMsg(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE_5, connectMessage));
            closeCtx(ctx, MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR);
        }
    }

    /**
     * 验证Sparkplug连接节点的Will主题是否为NDEATH类型
     */
    private SparkplugTopic validatedSparkplugTopicConnectedNode(MqttConnectMessage connectMessage) throws ThingsboardException {
        if (StringUtils.isNotBlank(connectMessage.payload().willTopic())
                && connectMessage.payload().willMessageInBytes() != null
                && connectMessage.payload().willMessageInBytes().length > 0) {
            SparkplugTopic sparkplugTopicNode = parseTopicPublish(connectMessage.payload().willTopic());
            if (NDEATH.equals(sparkplugTopicNode.getType())) {
                return sparkplugTopicNode;
            }
        }
        return null;
    }

    /**
     * 通道关闭完成回调 - 执行断开连接后的清理工作
     */
    @Override
    public void operationComplete(Future<? super Void> future) throws Exception {
        log.trace("[{}] Channel closed!", sessionId);
        doDisconnect();
    }

    /**
     * 执行断开连接逻辑
     */
    public void doDisconnect() {
        if (deviceSessionCtx.isConnected()) {
            log.debug("[{}] Client disconnected!", sessionId);
            // 通知传输服务会话关闭
            transportService.process(deviceSessionCtx.getSessionInfo(), SESSION_EVENT_MSG_CLOSED, null);
            transportService.deregisterSession(deviceSessionCtx.getSessionInfo());
            if (gatewaySessionHandler != null) {
                // 网关设备断开
                gatewaySessionHandler.onDevicesDisconnect();
            }
            if (sparkplugSessionHandler != null) {
                // Sparkplug节点发送离线状态
                // add Msg Telemetry node: key STATE type: String value: OFFLINE ts: sparkplugBProto.getTimestamp()
                sparkplugSessionHandler.sendSparkplugStateOnTelemetry(deviceSessionCtx.getSessionInfo(),
                        deviceSessionCtx.getDeviceInfo().getDeviceName(), OFFLINE, new Date().getTime());
                sparkplugSessionHandler.onDevicesDisconnect();
            }
            deviceSessionCtx.setDisconnected();
        }
        deviceSessionCtx.release();
    }

    /**
     * 处理设备认证响应
     */
    private void onValidateDeviceResponse(ValidateDeviceCredentialsResponse msg, ChannelHandlerContext ctx, MqttConnectMessage connectMessage) {
        if (!msg.hasDeviceInfo()) {
            // 认证失败
            context.onAuthFailure(address);
            MqttConnectReturnCode returnCode = MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED_5;
            if (sslHandler == null || getX509Certificate() == null) {
                // 根据认证信息细化错误类型
                String username = connectMessage.payload().userName();
                byte[] passwordBytes = connectMessage.payload().passwordInBytes();
                String clientId = connectMessage.payload().clientIdentifier();
                if ((username != null && passwordBytes != null && clientId != null)
                        || (username == null ^ passwordBytes == null)) {
                    returnCode = MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD;
                } else if (!StringUtils.isBlank(clientId)) {
                    returnCode = MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID;
                }
            }
            ctx.writeAndFlush(createMqttConnAckMsg(returnCode, connectMessage));
            closeCtx(ctx, returnCode);
        } else {
            // 认证成功
            context.onAuthSuccess(address);
            deviceSessionCtx.setDeviceInfo(msg.getDeviceInfo());
            deviceSessionCtx.setDeviceProfile(msg.getDeviceProfile());
            deviceSessionCtx.setSessionInfo(SessionInfoCreator.create(msg, context, sessionId));
            // 注册会话并处理连接成功
            transportService.process(deviceSessionCtx.getSessionInfo(), SESSION_EVENT_MSG_OPEN, new TransportServiceCallback<Void>() {
                @Override
                public void onSuccess(Void msg) {
                    SessionMetaData sessionMetaData = transportService.registerAsyncSession(deviceSessionCtx.getSessionInfo(), MqttTransportHandler.this);
                    if (deviceSessionCtx.isSparkplug()) {
                        checkSparkplugNodeSession(connectMessage, ctx, sessionMetaData);
                    } else {
                        checkGatewaySession(sessionMetaData);
                    }
                    ctx.writeAndFlush(createMqttConnAckMsg(MqttConnectReturnCode.CONNECTION_ACCEPTED, connectMessage));
                    deviceSessionCtx.setConnected(true);
                    log.debug("[{}] Client connected!", sessionId);
                    // 连接成功后处理队列中的消息
                    transportService.getCallbackExecutor().execute(() -> processMsgQueue(ctx)); //this callback will execute in Producer worker thread and hard or blocking work have to be submitted to the separate thread.
                }

                @Override
                public void onError(Throwable e) {
                    if (e instanceof TbRateLimitsException) {
                        // 速率限制异常
                        log.trace("[{}] Failed to submit session event: {}", sessionId, e.getMessage());
                        ctx.writeAndFlush(createMqttConnAckMsg(MqttConnectReturnCode.CONNECTION_REFUSED_CONNECTION_RATE_EXCEEDED, connectMessage));
                        closeCtx(ctx, MqttReasonCodes.Disconnect.MESSAGE_RATE_TOO_HIGH);
                    } else {
                        log.warn("[{}] Failed to submit session event", sessionId, e);
                        ctx.writeAndFlush(createMqttConnAckMsg(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE_5, connectMessage));
                        closeCtx(ctx, MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR);
                    }
                }
            });
        }
    }


    /* SessionMsgListener接口实现 - 处理从服务端到设备的消息 START*/

    /**
     * 收到属性获取响应
     */
    @Override
    public void onGetAttributesResponse(TransportProtos.GetAttributeResponseMsg response) {
        log.trace("[{}] Received get attributes response", sessionId);
        String topicBase = attrReqTopicType.getAttributesResponseTopicBase();
        MqttTransportAdaptor adaptor = deviceSessionCtx.getAdaptor(attrReqTopicType);
        try {
            adaptor.convertToPublish(deviceSessionCtx, response, topicBase).ifPresent(deviceSessionCtx.getChannel()::writeAndFlush);
        } catch (Exception e) {
            log.trace("[{}] Failed to convert device attributes response to MQTT msg", sessionId, e);
        }
    }

    /**
     * 收到属性更新通知
     */
    @Override
    public void onAttributeUpdate(UUID sessionId, TransportProtos.AttributeUpdateNotificationMsg notification) {
        log.trace("[{}] Received attributes update notification to device", sessionId);
        try {
            if (sparkplugSessionHandler != null) {
                // Sparkplug设备属性更新处理
                log.trace("[{}] Received attributes update notification to sparkplug device", sessionId);
                notification.getSharedUpdatedList().forEach(tsKvProto -> {
                    if (sparkplugSessionHandler.getNodeBirthMetrics().containsKey(tsKvProto.getKv().getKey())) {
                        SparkplugTopic sparkplugTopic = new SparkplugTopic(sparkplugSessionHandler.getSparkplugTopicNode(),
                                SparkplugMessageType.NCMD);
                        sparkplugSessionHandler.createSparkplugMqttPublishMsg(tsKvProto,
                                        sparkplugTopic.toString(),
                                        sparkplugSessionHandler.getNodeBirthMetrics().get(tsKvProto.getKv().getKey()))
                                .ifPresent(sparkplugSessionHandler::writeAndFlush);
                    }
                });
            } else {
                // 普通设备属性更新
                String topic = attrSubTopicType.getAttributesSubTopic();
                MqttTransportAdaptor adaptor = deviceSessionCtx.getAdaptor(attrSubTopicType);
                adaptor.convertToPublish(deviceSessionCtx, notification, topic).ifPresent(deviceSessionCtx.getChannel()::writeAndFlush);
            }
        } catch (Exception e) {
            log.trace("[{}] Failed to convert device attributes update to MQTT msg", sessionId, e);
        }
    }

    /**
     * 收到远程会话关闭命令
     */
    @Override
    public void onRemoteSessionCloseCommand(UUID sessionId, TransportProtos.SessionCloseNotificationProto sessionCloseNotification) {
        log.trace("[{}] Received the remote command to close the session: {}", sessionId, sessionCloseNotification.getMessage());
        transportService.deregisterSession(deviceSessionCtx.getSessionInfo());
        // 根据关闭原因映射到MQTT断开原因码
        MqttReasonCodes.Disconnect returnCode = switch (sessionCloseNotification.getReason()) {
            case CREDENTIALS_UPDATED, RPC_DELIVERY_TIMEOUT -> MqttReasonCodes.Disconnect.ADMINISTRATIVE_ACTION;
            case MAX_CONCURRENT_SESSIONS_LIMIT_REACHED -> MqttReasonCodes.Disconnect.SESSION_TAKEN_OVER;
            case SESSION_TIMEOUT -> MqttReasonCodes.Disconnect.MAXIMUM_CONNECT_TIME;
            default -> MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR;
        };
        closeCtx(deviceSessionCtx.getChannel(), returnCode);
    }


    /**
     * 收到发送到设备的RPC请求
     */
    @Override
    public void onToDeviceRpcRequest(UUID sessionId, TransportProtos.ToDeviceRpcRequestMsg rpcRequest) {
        log.trace("[{}][{}] Received RPC command to device: {}", deviceSessionCtx.getDeviceId(), sessionId, rpcRequest);
        try {
            if (sparkplugSessionHandler != null) {
                handleToSparkplugDeviceRpcRequest(rpcRequest);
            } else {
                String baseTopic = rpcSubTopicType.getRpcRequestTopicBase();
                MqttTransportAdaptor adaptor = deviceSessionCtx.getAdaptor(rpcSubTopicType);
                adaptor.convertToPublish(deviceSessionCtx, rpcRequest, baseTopic)
                        .ifPresent(payload -> sendToDeviceRpcRequest(payload, rpcRequest, deviceSessionCtx.getSessionInfo()));
            }
        } catch (Exception e) {
            log.trace("[{}][{}] Failed to convert device RPC command to MQTT msg", deviceSessionCtx.getDeviceId(), sessionId, e);
            this.sendErrorRpcResponse(deviceSessionCtx.getSessionInfo(), rpcRequest.getRequestId(),
                    ThingsboardErrorCode.INVALID_ARGUMENTS,
                    "Failed to convert device RPC command to MQTT msg: " + rpcRequest.getMethodName() + rpcRequest.getParams());
        }
    }

    /**
     * 处理获取会话限制的RPC请求（特殊RPC）
     */
    private void onGetSessionLimitsRpc(TransportProtos.SessionInfoProto sessionInfo, ChannelHandlerContext ctx, int msgId, TransportProtos.ToServerRpcRequestMsg rpcRequestMsg) {
        // 从租户配置缓存获取会话限制配置
        var tenantProfile = context.getTenantProfileCache().get(deviceSessionCtx.getTenantId());
        DefaultTenantProfileConfiguration profile = tenantProfile.getDefaultProfileConfiguration();

        SessionLimits sessionLimits;

        if (sessionInfo.getIsGateway()) {
            // 网关会话限制
            var gatewaySessionLimits = new GatewaySessionLimits();
            var gatewayLimits = new SessionLimits.SessionRateLimits(profile.getTransportGatewayMsgRateLimit(),
                    profile.getTransportGatewayTelemetryMsgRateLimit(),
                    profile.getTransportGatewayTelemetryDataPointsRateLimit());
            var gatewayDeviceLimits = new SessionLimits.SessionRateLimits(profile.getTransportGatewayDeviceMsgRateLimit(),
                    profile.getTransportGatewayDeviceTelemetryMsgRateLimit(),
                    profile.getTransportGatewayDeviceTelemetryDataPointsRateLimit());
            gatewaySessionLimits.setGatewayRateLimits(gatewayLimits);
            gatewaySessionLimits.setRateLimits(gatewayDeviceLimits);
            sessionLimits = gatewaySessionLimits;
        } else {
            // 普通设备会话限制
            var rateLimits = new SessionLimits.SessionRateLimits(profile.getTransportDeviceMsgRateLimit(),
                    profile.getTransportDeviceTelemetryMsgRateLimit(),
                    profile.getTransportDeviceTelemetryDataPointsRateLimit());
            sessionLimits = new SessionLimits();
            sessionLimits.setRateLimits(rateLimits);
        }
        sessionLimits.setMaxPayloadSize(context.getMaxPayloadSize());
        sessionLimits.setMaxInflightMessages(context.getMessageQueueSizePerDeviceLimit());

        ack(ctx, msgId, MqttReasonCodes.PubAck.SUCCESS);
        // 构建RPC响应
        TransportProtos.ToServerRpcResponseMsg responseMsg = TransportProtos.ToServerRpcResponseMsg.newBuilder()
                .setRequestId(rpcRequestMsg.getRequestId())
                .setPayload(JacksonUtil.toString(sessionLimits))
                .build();

        onToServerRpcResponse(responseMsg);
    }

    /**
     * 处理发送到Sparkplug设备的RPC请求
     */
    private void handleToSparkplugDeviceRpcRequest(TransportProtos.ToDeviceRpcRequestMsg rpcRequest) throws ThingsboardException {
        SparkplugMessageType messageType = SparkplugMessageType.parseMessageType(rpcRequest.getMethodName());
        SparkplugRpcRequestHeader header;
        if (StringUtils.isNotEmpty(rpcRequest.getParams())) {
            header = JacksonUtil.fromString(rpcRequest.getParams(), SparkplugRpcRequestHeader.class);
        } else {
            header = new SparkplugRpcRequestHeader();
        }
        header.setMessageType(messageType.name());
        TransportProtos.TsKvProto tsKvProto = getTsKvProto(header.getMetricName(), header.getValue(), new Date().getTime());
        if (sparkplugSessionHandler.getNodeBirthMetrics().containsKey(tsKvProto.getKv().getKey())) {
            SparkplugTopic sparkplugTopic = new SparkplugTopic(sparkplugSessionHandler.getSparkplugTopicNode(),
                    messageType);
            sparkplugSessionHandler.createSparkplugMqttPublishMsg(tsKvProto,
                            sparkplugTopic.toString(),
                            sparkplugSessionHandler.getNodeBirthMetrics().get(tsKvProto.getKv().getKey()))
                    .ifPresent(payload -> sendToDeviceRpcRequest(payload, rpcRequest, deviceSessionCtx.getSessionInfo()));
        } else {
            sendErrorRpcResponse(deviceSessionCtx.getSessionInfo(), rpcRequest.getRequestId(),
                    ThingsboardErrorCode.BAD_REQUEST_PARAMS, "Failed send To Node Rpc Request: " +
                            rpcRequest.getMethodName() + ". This node does not have a metricName: [" + tsKvProto.getKv().getKey() + "]");
        }
    }

    /**
     * 发送RPC请求到设备 - 包含ACK等待和超时处理
     */
    public void sendToDeviceRpcRequest(MqttMessage payload, TransportProtos.ToDeviceRpcRequestMsg rpcRequest, TransportProtos.SessionInfoProto sessionInfo) {
        int msgId = ((MqttPublishMessage) payload).variableHeader().packetId();
        int requestId = rpcRequest.getRequestId();
        if (isAckExpected(payload)) {
            // QoS > 0的消息需要等待ACK
            rpcAwaitingAck.put(msgId, rpcRequest);
            // 设置ACK超时定时器
            context.getScheduler().schedule(() -> {
                TransportProtos.ToDeviceRpcRequestMsg msg = rpcAwaitingAck.remove(msgId);
                if (msg != null) {
                    log.trace("[{}][{}][{}] Going to send to device actor RPC request TIMEOUT status update ...", deviceSessionCtx.getDeviceId(), sessionId, requestId);
                    transportService.process(sessionInfo, rpcRequest, RpcStatus.TIMEOUT, TransportServiceCallback.EMPTY);
                }
            }, Math.max(0, Math.min(deviceSessionCtx.getContext().getTimeout(), rpcRequest.getExpirationTime() - System.currentTimeMillis())), TimeUnit.MILLISECONDS);
        }
        var cf = publish(payload, deviceSessionCtx);
        cf.addListener(result -> {
            Throwable throwable = result.cause();
            if (throwable != null) {
                log.trace("[{}][{}][{}] Failed send RPC request to device due to: ", deviceSessionCtx.getDeviceId(), sessionId, requestId, throwable);
                this.sendErrorRpcResponse(sessionInfo, requestId,
                        ThingsboardErrorCode.INVALID_ARGUMENTS, " Failed send To Device Rpc Request: " + rpcRequest.getMethodName());
                return;
            }
            if (!isAckExpected(payload)) {
                // QoS 0的消息直接标记为已送达
                log.trace("[{}][{}][{}] Going to send to device actor RPC request DELIVERED status update ...", deviceSessionCtx.getDeviceId(), sessionId, requestId);
                transportService.process(sessionInfo, rpcRequest, RpcStatus.DELIVERED, TransportServiceCallback.EMPTY);
            } else if (rpcRequest.getPersisted()) {
                // 持久化RPC请求标记为已发送
                log.trace("[{}][{}][{}] Going to send to device actor RPC request SENT status update ...", deviceSessionCtx.getDeviceId(), sessionId, requestId);
                transportService.process(sessionInfo, rpcRequest, RpcStatus.SENT, TransportServiceCallback.EMPTY);
            }
            if (sparkplugSessionHandler != null) {
                this.sendSuccessRpcResponse(sessionInfo, requestId, ResponseCode.CONTENT, "Success: " + rpcRequest.getMethodName());
            }
        });
    }

    /**
     * 收到服务端RPC响应
     */
    @Override
    public void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg rpcResponse) {
        log.trace("[{}] Received RPC response from server", sessionId);
        String baseTopic = toServerRpcSubTopicType.getRpcResponseTopicBase();
        MqttTransportAdaptor adaptor = deviceSessionCtx.getAdaptor(toServerRpcSubTopicType);
        try {
            adaptor.convertToPublish(deviceSessionCtx, rpcResponse, baseTopic).ifPresent(deviceSessionCtx.getChannel()::writeAndFlush);
        } catch (Exception e) {
            log.trace("[{}] Failed to convert device RPC command to MQTT msg", sessionId, e);
        }
    }

    /**
     * 设备配置更新回调
     */
    @Override
    public void onDeviceProfileUpdate(TransportProtos.SessionInfoProto sessionInfo, DeviceProfile deviceProfile) {
        deviceSessionCtx.onDeviceProfileUpdate(sessionInfo, deviceProfile);
    }

    /**
     * 设备信息更新回调
     */
    @Override
    public void onDeviceUpdate(TransportProtos.SessionInfoProto sessionInfo, Device device, Optional<DeviceProfile> deviceProfileOpt) {
        deviceSessionCtx.onDeviceUpdate(sessionInfo, device, deviceProfileOpt);
        if (gatewaySessionHandler != null) {
            gatewaySessionHandler.onGatewayUpdate(sessionInfo, device, deviceProfileOpt);
        }
    }

    /**
     * 设备删除回调
     */
    @Override
    public void onDeviceDeleted(DeviceId deviceId) {
        context.onAuthFailure(address);
        ChannelHandlerContext ctx = deviceSessionCtx.getChannel();
        closeCtx(ctx, MqttReasonCodes.Disconnect.ADMINISTRATIVE_ACTION);
        if (gatewaySessionHandler != null) {
            gatewaySessionHandler.onGatewayDelete(deviceId);
        }
    }

    /* SessionMsgListener接口实现 - 处理从服务端到设备的消息 END*/

    /**
     * 发送错误RPC响应
     */
    public void sendErrorRpcResponse(TransportProtos.SessionInfoProto sessionInfo, int requestId, ThingsboardErrorCode result, String errorMsg) {
        String payload = JacksonUtil.toString(SparkplugRpcResponseBody.builder().result(result.name()).error(errorMsg).build());
        TransportProtos.ToDeviceRpcResponseMsg msg = TransportProtos.ToDeviceRpcResponseMsg.newBuilder().setRequestId(requestId).setError(payload).build();
        transportService.process(sessionInfo, msg, null);
    }

    /**
     * 发送成功RPC响应
     */
    public void sendSuccessRpcResponse(TransportProtos.SessionInfoProto sessionInfo, int requestId, ResponseCode result, String successMsg) {
        String payload = JacksonUtil.toString(SparkplugRpcResponseBody.builder().result(result.getName()).result(successMsg).build());
        TransportProtos.ToDeviceRpcResponseMsg msg = TransportProtos.ToDeviceRpcResponseMsg.newBuilder().setRequestId(requestId).setError(payload).build();
        transportService.process(sessionInfo, msg, null);
    }

    /**
     * 发布MQTT消息到设备
     */
    private ChannelFuture publish(MqttMessage message, DeviceSessionCtx deviceSessionCtx) {
        return deviceSessionCtx.getChannel().writeAndFlush(message);
    }

    /**
     * 检查是否期望ACK（QoS > 0）
     */
    private boolean isAckExpected(MqttMessage message) {
        return message.fixedHeader().qosLevel().value() > 0;
    }

}
