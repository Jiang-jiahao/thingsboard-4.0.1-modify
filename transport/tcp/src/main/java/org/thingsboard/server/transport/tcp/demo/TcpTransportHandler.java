//package org.thingsboard.server.transport.tcp.adaptors;
//
//import io.netty.channel.ChannelHandlerContext;
//import io.netty.channel.ChannelInboundHandlerAdapter;
//import io.netty.handler.timeout.IdleStateEvent;
//import lombok.extern.slf4j.Slf4j;
//import org.thingsboard.server.common.adaptor.AdaptorException;
//import org.thingsboard.server.common.data.Device;
//import org.thingsboard.server.common.data.DeviceProfile;
//import org.thingsboard.server.common.data.DeviceTransportType;
//import org.thingsboard.server.common.data.id.DeviceId;
//import org.thingsboard.server.common.msg.TbMsgMetaData;
//import org.thingsboard.server.common.transport.SessionMsgListener;
//import org.thingsboard.server.common.transport.TransportService;
//import org.thingsboard.server.common.transport.TransportServiceCallback;
//import org.thingsboard.server.common.transport.auth.SessionInfoCreator;
//import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
//import org.thingsboard.server.common.transport.service.SessionMetaData;
//import org.thingsboard.server.gen.transport.TransportProtos;
//import org.thingsboard.server.transport.tcp.TcpTransportContext;
//import org.thingsboard.server.transport.tcp.adaptors.TcpAdaptor;
//import org.thingsboard.server.transport.tcp.session.DeviceSessionCtx;
//import org.thingsboard.server.transport.tcp.session.TcpSessionHandler;
//import org.thingsboard.server.transport.tcp.util.TcpMessage;
//import org.thingsboard.server.transport.tcp.util.TcpMessageType;
//
//import java.net.InetSocketAddress;
//import java.util.Optional;
//import java.util.UUID;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//
//@Slf4j
//public class TcpTransportHandler extends ChannelInboundHandlerAdapter implements SessionMsgListener {
//
//    private final UUID sessionId;
//    private final TcpTransportContext context;
//    private final TransportService transportService;
//    private final DeviceSessionCtx deviceSessionCtx;
//    private final ConcurrentMap<Integer, TransportProtos.ToDeviceRpcRequestMsg> rpcAwaitingAck;
//
//    private volatile InetSocketAddress address;
//    private volatile TcpSessionHandler sessionHandler;
//    private volatile boolean authenticated;
//    private volatile long lastActivityTime;
//
//    public TcpTransportHandler(TcpTransportContext context) {
//        this.sessionId = UUID.randomUUID();
//        this.context = context;
//        this.transportService = context.getTransportService();
//        this.deviceSessionCtx = new DeviceSessionCtx(sessionId, context);
//        this.rpcAwaitingAck = new ConcurrentHashMap<>();
//        this.lastActivityTime = System.currentTimeMillis();
//    }
//
//    @Override
//    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
//        super.channelRegistered(ctx);
//        context.channelRegistered();
//        log.debug("[{}] Channel registered", sessionId);
//    }
//
//    @Override
//    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
//        super.channelUnregistered(ctx);
//        context.channelUnregistered();
//        log.debug("[{}] Channel unregistered", sessionId);
//    }
//
//    @Override
//    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        super.channelActive(ctx);
//        address = (InetSocketAddress) ctx.channel().remoteAddress();
//        log.debug("[{}] Client connected from {}", sessionId, address);
//    }
//
//    @Override
//    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//        super.channelInactive(ctx);
//        log.debug("[{}] Client disconnected", sessionId);
//        doDisconnect();
//    }
//
//    @Override
//    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        lastActivityTime = System.currentTimeMillis();
//
//        if (msg instanceof byte[]) {
//            byte[] data = (byte[]) msg;
//            log.trace("[{}] Received {} bytes", sessionId, data.length);
//
//            try {
//                // 解析TCP消息
//                TcpMessage tcpMsg = TcpMessage.decode(data);
//                processTcpMessage(ctx, tcpMsg);
//            } catch (Exception e) {
//                log.warn("[{}] Failed to process TCP message", sessionId, e);
//                sendErrorResponse(ctx, TcpMessageType.ERROR, "Invalid message format");
//            }
//        } else {
//            log.warn("[{}] Unsupported message type: {}", sessionId, msg.getClass());
//            ctx.close();
//        }
//    }
//
//    private void processTcpMessage(ChannelHandlerContext ctx, TcpMessage tcpMsg) {
//        if (!authenticated) {
//            // 如果第一次没有认证，那么直接将数据传输
//            if (tcpMsg.getType() == TcpMessageType.CONNECT) {
//                processConnect(ctx, tcpMsg);
//            } else {
//                sendErrorResponse(ctx, TcpMessageType.ERROR, "Authentication required");
//                ctx.close();
//            }
//        } else {
//            // 处理已认证的消息
//            processAuthenticatedMessage(ctx, tcpMsg);
//        }
//    }
//
//    private void processConnect(ChannelHandlerContext ctx, TcpMessage connectMsg) {
//        try {
//            String token = connectMsg.getPayload().toString();
//            log.debug("[{}] Processing connect with token: {}", sessionId, token);
//
//            // 验证设备凭证
//            transportService.process(DeviceTransportType.DEFAULT,
//                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder()
//                    .setToken(token)
//                    .build(),
//                new TransportServiceCallback<>() {
//                    @Override
//                    public void onSuccess(ValidateDeviceCredentialsResponse response) {
//                        if (response.hasDeviceInfo()) {
//                            onDeviceAuthSuccess(ctx, response);
//                        } else {
//                            sendErrorResponse(ctx, TcpMessageType.CONNACK,
//                                "Authentication failed");
//                            ctx.close();
//                        }
//                    }
//
//                    @Override
//                    public void onError(Throwable e) {
//                        log.warn("[{}] Failed to validate device credentials", sessionId, e);
//                        sendErrorResponse(ctx, TcpMessageType.CONNACK,
//                            "Internal server error");
//                        ctx.close();
//                    }
//                });
//        } catch (Exception e) {
//            log.warn("[{}] Failed to process connect message", sessionId, e);
//            sendErrorResponse(ctx, TcpMessageType.CONNACK, "Invalid connect message");
//            ctx.close();
//        }
//    }
//
//    private void onDeviceAuthSuccess(ChannelHandlerContext ctx,
//                                   ValidateDeviceCredentialsResponse response) {
//        deviceSessionCtx.setDeviceInfo(response.getDeviceInfo());
//        deviceSessionCtx.setDeviceProfile(response.getDeviceProfile());
//        deviceSessionCtx.setSessionInfo(
//            SessionInfoCreator.create(response, context, sessionId)
//        );
//
//        // 注册会话
//        transportService.process(deviceSessionCtx.getSessionInfo(),
//            TransportProtos.SessionEventMsg.newBuilder()
//                .setSessionType(TransportProtos.SessionType.ASYNC)
//                .setEvent(TransportProtos.SessionEvent.OPEN)
//                .build(),
//            new TransportServiceCallback<>() {
//                @Override
//                public void onSuccess(Void msg) {
//                    SessionMetaData sessionMetaData = transportService.registerAsyncSession(
//                        deviceSessionCtx.getSessionInfo(), TcpTransportHandler.this);
//
//                    authenticated = true;
//
//                    // 创建会话处理器
//                    sessionHandler = new TcpSessionHandler(
//                        deviceSessionCtx, sessionId, TcpTransportHandler.this);
//
//                    // 发送连接确认
//                    sendSuccessResponse(ctx, TcpMessageType.CONNACK,
//                        "Authentication successful");
//
//                    log.info("[{}][{}] Device authenticated successfully",
//                        sessionId, deviceSessionCtx.getDeviceId());
//                }
//
//                @Override
//                public void onError(Throwable e) {
//                    log.warn("[{}] Failed to register session", sessionId, e);
//                    sendErrorResponse(ctx, TcpMessageType.CONNACK,
//                        "Failed to create session");
//                    ctx.close();
//                }
//            });
//    }
//
//    private void processAuthenticatedMessage(ChannelHandlerContext ctx, TcpMessage tcpMsg) {
//        try {
//            TcpAdaptor adaptor = deviceSessionCtx.getAdaptor();
//
//            switch (tcpMsg.getType()) {
//                case TELEMETRY:
//                    TransportProtos.PostTelemetryMsg telemetryMsg =
//                        adaptor.convertToTelemetry(deviceSessionCtx, tcpMsg.getPayload());
//                    processTelemetry(ctx, telemetryMsg, tcpMsg.getMessageId());
//                    break;
//
//                case ATTRIBUTES:
//                    TransportProtos.PostAttributeMsg attributeMsg =
//                        adaptor.convertToAttributes(deviceSessionCtx, tcpMsg.getPayload());
//                    processAttributes(ctx, attributeMsg, tcpMsg.getMessageId());
//                    break;
//
//                case RPC_RESPONSE:
//                    TransportProtos.ToDeviceRpcResponseMsg rpcResponseMsg =
//                        adaptor.convertToRpcResponse(deviceSessionCtx, tcpMsg.getPayload());
//                    processRpcResponse(ctx, rpcResponseMsg, tcpMsg.getMessageId());
//                    break;
//
//                case RPC_REQUEST:
//                    TransportProtos.ToServerRpcRequestMsg rpcRequestMsg =
//                        adaptor.convertToServerRpcRequest(deviceSessionCtx, tcpMsg.getPayload());
//                    processRpcRequest(ctx, rpcRequestMsg, tcpMsg.getMessageId());
//                    break;
//
//                case PING:
//                    processPing(ctx, tcpMsg.getMessageId());
//                    break;
//
//                case DISCONNECT:
//                    processDisconnect(ctx);
//                    break;
//
//                default:
//                    log.warn("[{}] Unsupported message type: {}",
//                        sessionId, tcpMsg.getType());
//                    sendErrorResponse(ctx, TcpMessageType.ERROR,
//                        "Unsupported message type");
//            }
//        } catch (AdaptorException e) {
//            log.warn("[{}] Failed to process message", sessionId, e);
//            sendErrorResponse(ctx, TcpMessageType.ERROR,
//                "Failed to process message: " + e.getMessage());
//        }
//    }
//
//    private void processTelemetry(ChannelHandlerContext ctx,
//                                TransportProtos.PostTelemetryMsg msg,
//                                int messageId) {
//        transportService.process(deviceSessionCtx.getSessionInfo(), msg,
//            new TbMsgMetaData(),
//            new TransportServiceCallback<>() {
//                @Override
//                public void onSuccess(Void dummy) {
//                    sendSuccessResponse(ctx, TcpMessageType.ACK, messageId);
//                }
//
//                @Override
//                public void onError(Throwable e) {
//                    log.warn("[{}] Failed to process telemetry", sessionId, e);
//                    sendErrorResponse(ctx, TcpMessageType.ERROR, messageId,
//                        "Failed to process telemetry");
//                }
//            });
//    }
//
//    private void processAttributes(ChannelHandlerContext ctx,
//                                 TransportProtos.PostAttributeMsg msg,
//                                 int messageId) {
//        transportService.process(deviceSessionCtx.getSessionInfo(), msg,
//            new TbMsgMetaData(),
//            new TransportServiceCallback<>() {
//                @Override
//                public void onSuccess(Void dummy) {
//                    sendSuccessResponse(ctx, TcpMessageType.ACK, messageId);
//                }
//
//                @Override
//                public void onError(Throwable e) {
//                    log.warn("[{}] Failed to process attributes", sessionId, e);
//                    sendErrorResponse(ctx, TcpMessageType.ERROR, messageId,
//                        "Failed to process attributes");
//                }
//            });
//    }
//
//    private void processRpcResponse(ChannelHandlerContext ctx,
//                                  TransportProtos.ToDeviceRpcResponseMsg msg,
//                                  int messageId) {
//        transportService.process(deviceSessionCtx.getSessionInfo(), msg,
//            new TransportServiceCallback<>() {
//                @Override
//                public void onSuccess(Void dummy) {
//                    sendSuccessResponse(ctx, TcpMessageType.ACK, messageId);
//                }
//
//                @Override
//                public void onError(Throwable e) {
//                    log.warn("[{}] Failed to process RPC response", sessionId, e);
//                    sendErrorResponse(ctx, TcpMessageType.ERROR, messageId,
//                        "Failed to process RPC response");
//                }
//            });
//    }
//
//    private void processRpcRequest(ChannelHandlerContext ctx,
//                                 TransportProtos.ToServerRpcRequestMsg msg,
//                                 int messageId) {
//        transportService.process(deviceSessionCtx.getSessionInfo(), msg,
//            new TransportServiceCallback<>() {
//                @Override
//                public void onSuccess(Void dummy) {
//                    sendSuccessResponse(ctx, TcpMessageType.ACK, messageId);
//                }
//
//                @Override
//                public void onError(Throwable e) {
//                    log.warn("[{}] Failed to process RPC request", sessionId, e);
//                    sendErrorResponse(ctx, TcpMessageType.ERROR, messageId,
//                        "Failed to process RPC request");
//                }
//            });
//    }
//
//    private void processPing(ChannelHandlerContext ctx, int messageId) {
//        sendSuccessResponse(ctx, TcpMessageType.PONG, messageId);
//        transportService.recordActivity(deviceSessionCtx.getSessionInfo());
//    }
//
//    private void processDisconnect(ChannelHandlerContext ctx) {
//        sendSuccessResponse(ctx, TcpMessageType.DISCONNACK, 0);
//        ctx.close();
//    }
//
//    @Override
//    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
//        if (evt instanceof IdleStateEvent) {
//            log.debug("[{}] Idle state detected, closing connection", sessionId);
//            ctx.close();
//        }
//    }
//
//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        log.error("[{}] TCP transport error", sessionId, cause);
//        ctx.close();
//    }
//
//    private void sendSuccessResponse(ChannelHandlerContext ctx,
//                                   TcpMessageType type, int messageId) {
//        TcpMessage response = TcpMessage.success(type, messageId);
//        ctx.writeAndFlush(response.encode());
//    }
//
//    private void sendSuccessResponse(ChannelHandlerContext ctx,
//                                   TcpMessageType type, String message) {
//        TcpMessage response = TcpMessage.success(type, message);
//        ctx.writeAndFlush(response.encode());
//    }
//
//    private void sendErrorResponse(ChannelHandlerContext ctx,
//                                 TcpMessageType type, int messageId,
//                                 String error) {
//        TcpMessage response = TcpMessage.error(type, messageId, error);
//        ctx.writeAndFlush(response.encode());
//    }
//
//    private void sendErrorResponse(ChannelHandlerContext ctx,
//                                 TcpMessageType type, String error) {
//        TcpMessage response = TcpMessage.error(type, 0, error);
//        ctx.writeAndFlush(response.encode());
//    }
//
//    private void doDisconnect() {
//        if (authenticated && deviceSessionCtx.getSessionInfo() != null) {
//            transportService.process(deviceSessionCtx.getSessionInfo(),
//                TransportProtos.SessionEventMsg.newBuilder()
//                    .setSessionType(TransportProtos.SessionType.ASYNC)
//                    .setEvent(TransportProtos.SessionEvent.CLOSED)
//                    .build(),
//                null);
//            transportService.deregisterSession(deviceSessionCtx.getSessionInfo());
//            authenticated = false;
//        }
//    }
//
//    // SessionMsgListener implementation
//    @Override
//    public void onGetAttributesResponse(TransportProtos.GetAttributeResponseMsg response) {
//        if (sessionHandler != null) {
//            sessionHandler.onGetAttributesResponse(response);
//        }
//    }
//
//    @Override
//    public void onAttributeUpdate(UUID sessionId,
//                                 TransportProtos.AttributeUpdateNotificationMsg notification) {
//        if (sessionHandler != null) {
//            sessionHandler.onAttributeUpdate(sessionId, notification);
//        }
//    }
//
//    @Override
//    public void onRemoteSessionCloseCommand(UUID sessionId,
//                                          TransportProtos.SessionCloseNotificationProto sessionCloseNotification) {
//        if (sessionHandler != null) {
//            sessionHandler.onRemoteSessionCloseCommand(sessionId, sessionCloseNotification);
//        }
//    }
//
//    @Override
//    public void onToDeviceRpcRequest(UUID sessionId,
//                                   TransportProtos.ToDeviceRpcRequestMsg rpcRequest) {
//        if (sessionHandler != null) {
//            sessionHandler.onToDeviceRpcRequest(sessionId, rpcRequest);
//        }
//    }
//
//    @Override
//    public void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg rpcResponse) {
//        if (sessionHandler != null) {
//            sessionHandler.onToServerRpcResponse(rpcResponse);
//        }
//    }
//
//    @Override
//    public void onDeviceProfileUpdate(TransportProtos.SessionInfoProto sessionInfo,
//                                    DeviceProfile deviceProfile) {
//        if (sessionHandler != null) {
//            sessionHandler.onDeviceProfileUpdate(sessionInfo, deviceProfile);
//        }
//    }
//
//    @Override
//    public void onDeviceUpdate(TransportProtos.SessionInfoProto sessionInfo,
//                             Device device, Optional<DeviceProfile> deviceProfileOpt) {
//        if (sessionHandler != null) {
//            sessionHandler.onDeviceUpdate(sessionInfo, device, deviceProfileOpt);
//        }
//    }
//
//    @Override
//    public void onDeviceDeleted(DeviceId deviceId) {
//        if (sessionHandler != null) {
//            sessionHandler.onDeviceDeleted(deviceId);
//        }
//    }
//
//    public ChannelHandlerContext getChannel() {
//        return deviceSessionCtx.getChannel();
//    }
//}