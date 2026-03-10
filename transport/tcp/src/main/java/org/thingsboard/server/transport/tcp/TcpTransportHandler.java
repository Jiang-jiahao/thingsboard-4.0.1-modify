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
package org.thingsboard.server.transport.tcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonParseException;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.leshan.core.ResponseCode;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.adaptor.AdaptorException;
import org.thingsboard.server.common.data.*;
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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.netty.handler.codec.mqtt.MqttMessageType.*;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static org.thingsboard.server.common.transport.service.DefaultTransportService.*;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugConnectionState.OFFLINE;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMessageType.NDEATH;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugMetricUtil.getTsKvProto;
import static org.thingsboard.server.transport.mqtt.util.sparkplug.SparkplugTopicUtil.parseTopicPublish;

/**
 * @author Andrew Shvayka
 */
@Slf4j
public class TcpTransportHandler extends ChannelInboundHandlerAdapter implements GenericFutureListener<Future<? super Void>>, SessionMsgListener {

    private static final Pattern FW_REQUEST_PATTERN = Pattern.compile(MqttTopics.DEVICE_FIRMWARE_REQUEST_TOPIC_PATTERN);
    private static final Pattern SW_REQUEST_PATTERN = Pattern.compile(MqttTopics.DEVICE_SOFTWARE_REQUEST_TOPIC_PATTERN);

    private static final String SESSION_LIMITS = "getSessionLimits";

    private static final String PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE";

    private static final MqttQoS MAX_SUPPORTED_QOS_LVL = AT_LEAST_ONCE;

    private final UUID sessionId;

    protected final TcpTransportContext context;
    private final TransportService transportService;
    private final SchedulerComponent scheduler;

    final DeviceSessionCtx deviceSessionCtx;
    volatile InetSocketAddress address;
    volatile GatewaySessionHandler gatewaySessionHandler;
    volatile SparkplugNodeSessionHandler sparkplugSessionHandler;

    private final ConcurrentHashMap<String, String> otaPackSessions;
    private final ConcurrentHashMap<String, Integer> chunkSizes;
    private final ConcurrentMap<Integer, TransportProtos.ToDeviceRpcRequestMsg> rpcAwaitingAck;


    TcpTransportHandler(TcpTransportContext context) {
        this.sessionId = UUID.randomUUID();
        this.context = context;
        this.transportService = context.getTransportService();
        this.scheduler = context.getScheduler();
        this.deviceSessionCtx = new DeviceSessionCtx(sessionId, mqttQoSMap, context);
        this.otaPackSessions = new ConcurrentHashMap<>();
        this.chunkSizes = new ConcurrentHashMap<>();
        this.rpcAwaitingAck = new ConcurrentHashMap<>();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        context.channelRegistered();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        context.channelUnregistered();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.trace("[{}] Processing msg: {}", sessionId, msg);
        if (address == null) {
            address = getAddress(ctx);
        }
        try {

        } finally {
            ReferenceCountUtil.safeRelease(msg);
        }
    }

    private void closeCtx(ChannelHandlerContext ctx, MqttReasonCodes.Disconnect returnCode) {
        closeCtx(ctx, returnCode.byteValue());
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
        var address = ctx.channel().attr(TcpTransportService.ADDRESS).get();
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


    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof IOException) {
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
            log.error("Received critical error. Going to shutdown the service.");
            System.exit(1);
        }
    }



    public static MqttMessage createMqttDisconnectMsg(DeviceSessionCtx deviceSessionCtx, byte returnCode) {
        MqttMessageBuilders.DisconnectBuilder disconnectBuilder = MqttMessageBuilders.disconnect();
        if (MqttVersion.MQTT_5.equals(deviceSessionCtx.getMqttVersion())) {
            disconnectBuilder.reasonCode(returnCode);
        }
        return disconnectBuilder.build();
    }




    @Override
    public void operationComplete(Future<? super Void> future) throws Exception {
        log.trace("[{}] Channel closed!", sessionId);
        doDisconnect();
    }

    public void doDisconnect() {
        if (deviceSessionCtx.isConnected()) {
            log.debug("[{}] Client disconnected!", sessionId);
            transportService.process(deviceSessionCtx.getSessionInfo(), SESSION_EVENT_MSG_CLOSED, null);
            transportService.deregisterSession(deviceSessionCtx.getSessionInfo());
            if (gatewaySessionHandler != null) {
                gatewaySessionHandler.onDevicesDisconnect();
            }
            if (sparkplugSessionHandler != null) {
                // add Msg Telemetry node: key STATE type: String value: OFFLINE ts: sparkplugBProto.getTimestamp()
                sparkplugSessionHandler.sendSparkplugStateOnTelemetry(deviceSessionCtx.getSessionInfo(),
                        deviceSessionCtx.getDeviceInfo().getDeviceName(), OFFLINE, new Date().getTime());
                sparkplugSessionHandler.onDevicesDisconnect();
            }
            deviceSessionCtx.setDisconnected();
        }
        deviceSessionCtx.release();
    }


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

    @Override
    public void onAttributeUpdate(UUID sessionId, TransportProtos.AttributeUpdateNotificationMsg notification) {
        log.trace("[{}] Received attributes update notification to device", sessionId);
        try {
            if (sparkplugSessionHandler != null) {
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
                String topic = attrSubTopicType.getAttributesSubTopic();
                MqttTransportAdaptor adaptor = deviceSessionCtx.getAdaptor(attrSubTopicType);
                adaptor.convertToPublish(deviceSessionCtx, notification, topic).ifPresent(deviceSessionCtx.getChannel()::writeAndFlush);
            }
        } catch (Exception e) {
            log.trace("[{}] Failed to convert device attributes update to MQTT msg", sessionId, e);
        }
    }

    @Override
    public void onRemoteSessionCloseCommand(UUID sessionId, TransportProtos.SessionCloseNotificationProto sessionCloseNotification) {
        log.trace("[{}] Received the remote command to close the session: {}", sessionId, sessionCloseNotification.getMessage());
        transportService.deregisterSession(deviceSessionCtx.getSessionInfo());
        MqttReasonCodes.Disconnect returnCode = switch (sessionCloseNotification.getReason()) {
            case CREDENTIALS_UPDATED, RPC_DELIVERY_TIMEOUT -> MqttReasonCodes.Disconnect.ADMINISTRATIVE_ACTION;
            case MAX_CONCURRENT_SESSIONS_LIMIT_REACHED -> MqttReasonCodes.Disconnect.SESSION_TAKEN_OVER;
            case SESSION_TIMEOUT -> MqttReasonCodes.Disconnect.MAXIMUM_CONNECT_TIME;
            default -> MqttReasonCodes.Disconnect.IMPLEMENTATION_SPECIFIC_ERROR;
        };
        closeCtx(deviceSessionCtx.getChannel(), returnCode);
    }

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

    public void sendToDeviceRpcRequest(MqttMessage payload, TransportProtos.ToDeviceRpcRequestMsg rpcRequest, TransportProtos.SessionInfoProto sessionInfo) {
        int msgId = ((MqttPublishMessage) payload).variableHeader().packetId();
        int requestId = rpcRequest.getRequestId();
        if (isAckExpected(payload)) {
            rpcAwaitingAck.put(msgId, rpcRequest);
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
                log.trace("[{}][{}][{}] Going to send to device actor RPC request DELIVERED status update ...", deviceSessionCtx.getDeviceId(), sessionId, requestId);
                transportService.process(sessionInfo, rpcRequest, RpcStatus.DELIVERED, TransportServiceCallback.EMPTY);
            } else if (rpcRequest.getPersisted()) {
                log.trace("[{}][{}][{}] Going to send to device actor RPC request SENT status update ...", deviceSessionCtx.getDeviceId(), sessionId, requestId);
                transportService.process(sessionInfo, rpcRequest, RpcStatus.SENT, TransportServiceCallback.EMPTY);
            }
            if (sparkplugSessionHandler != null) {
                this.sendSuccessRpcResponse(sessionInfo, requestId, ResponseCode.CONTENT, "Success: " + rpcRequest.getMethodName());
            }
        });
    }

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

    private ChannelFuture publish(MqttMessage message, DeviceSessionCtx deviceSessionCtx) {
        return deviceSessionCtx.getChannel().writeAndFlush(message);
    }

    private boolean isAckExpected(MqttMessage message) {
        return message.fixedHeader().qosLevel().value() > 0;
    }

    @Override
    public void onDeviceProfileUpdate(TransportProtos.SessionInfoProto sessionInfo, DeviceProfile deviceProfile) {
        deviceSessionCtx.onDeviceProfileUpdate(sessionInfo, deviceProfile);
    }

    @Override
    public void onDeviceUpdate(TransportProtos.SessionInfoProto sessionInfo, Device device, Optional<DeviceProfile> deviceProfileOpt) {
        deviceSessionCtx.onDeviceUpdate(sessionInfo, device, deviceProfileOpt);
        if (gatewaySessionHandler != null) {
            gatewaySessionHandler.onGatewayUpdate(sessionInfo, device, deviceProfileOpt);
        }
    }

    @Override
    public void onDeviceDeleted(DeviceId deviceId) {
        context.onAuthFailure(address);
        ChannelHandlerContext ctx = deviceSessionCtx.getChannel();
        closeCtx(ctx, MqttReasonCodes.Disconnect.ADMINISTRATIVE_ACTION);
        if (gatewaySessionHandler != null) {
            gatewaySessionHandler.onGatewayDelete(deviceId);
        }
    }

    public void sendErrorRpcResponse(TransportProtos.SessionInfoProto sessionInfo, int requestId, ThingsboardErrorCode result, String errorMsg) {
        String payload = JacksonUtil.toString(SparkplugRpcResponseBody.builder().result(result.name()).error(errorMsg).build());
        TransportProtos.ToDeviceRpcResponseMsg msg = TransportProtos.ToDeviceRpcResponseMsg.newBuilder().setRequestId(requestId).setError(payload).build();
        transportService.process(sessionInfo, msg, null);
    }

    public void sendSuccessRpcResponse(TransportProtos.SessionInfoProto sessionInfo, int requestId, ResponseCode result, String successMsg) {
        String payload = JacksonUtil.toString(SparkplugRpcResponseBody.builder().result(result.getName()).result(successMsg).build());
        TransportProtos.ToDeviceRpcResponseMsg msg = TransportProtos.ToDeviceRpcResponseMsg.newBuilder().setRequestId(requestId).setError(payload).build();
        transportService.process(sessionInfo, msg, null);
    }

}
