//package org.thingsboard.server.transport.tcp.session;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import io.netty.channel.ChannelHandlerContext;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.thingsboard.common.util.JacksonUtil;
//import org.thingsboard.server.common.data.Device;
//import org.thingsboard.server.common.data.DeviceProfile;
//import org.thingsboard.server.common.data.id.DeviceId;
//import org.thingsboard.server.common.transport.SessionMsgListener;
//import org.thingsboard.server.gen.transport.TransportProtos;
//import org.thingsboard.server.transport.tcp.util.TcpMessageType;
//import org.thingsboard.server.transport.tcp.util.TcpMessage;
//
//import java.util.Optional;
//import java.util.UUID;
//
///**
// * TCP会话业务处理器，实现平台到设备的消息下发
// */
//@Slf4j
//@RequiredArgsConstructor
//public class TcpSessionHandler implements SessionMsgListener {
//
//    private final DeviceSessionCtx sessionCtx;
//    private final UUID handlerId = UUID.randomUUID();
//
//    @Override
//    public void onGetAttributesResponse(TransportProtos.GetAttributeResponseMsg response) {
//        log.debug("[{}][{}] 收到属性查询响应，请求ID: {}",
//                 handlerId, sessionCtx.getSessionId(), response.getRequestId());
//
//        try {
//            // 转换为TCP消息并发送给设备
//            TcpMessage msg = convertToTcpMessage(response);
//            sendToDevice(msg);
//            sessionCtx.incrementAttributeCount();
//        } catch (Exception e) {
//            log.warn("[{}][{}] 处理属性响应失败", handlerId, sessionCtx.getSessionId(), e);
//        }
//    }
//
//    @Override
//    public void onAttributeUpdate(UUID sessionId, TransportProtos.AttributeUpdateNotificationMsg notification) {
//        log.debug("[{}][{}] 收到属性更新通知，共享属性: {}, 客户端属性: {}",
//                 handlerId, sessionCtx.getSessionId(),
//                 notification.getSharedUpdatedCount(),
//                 notification.getSharedUpdatedCount());
//
//        try {
//            // 转换为TCP消息并发送给设备
//            TcpMessage msg = convertToTcpMessage(notification);
//            sendToDevice(msg);
//            sessionCtx.incrementAttributeCount();
//        } catch (Exception e) {
//            log.warn("[{}][{}] 处理属性更新失败", handlerId, sessionCtx.getSessionId(), e);
//        }
//    }
//
//    @Override
//    public void onToDeviceRpcRequest(UUID sessionId, TransportProtos.ToDeviceRpcRequestMsg rpcRequest) {
//        log.debug("[{}][{}] 收到RPC请求，设备: {}, 请求ID: {}, 方法: {}",
//                 handlerId, sessionCtx.getSessionId(),
//                 sessionCtx.getDeviceId(), rpcRequest.getRequestId(), rpcRequest.getMethodName());
//
//        try {
//            // 转换为TCP RPC请求消息
//            TcpMessage msg = convertToTcpMessage(rpcRequest);
//            sendToDevice(msg);
//            sessionCtx.incrementRpcCount();
//
//            log.debug("[{}][{}] RPC请求已下发到设备", handlerId, sessionCtx.getSessionId());
//        } catch (Exception e) {
//            log.warn("[{}][{}] 处理RPC请求失败", handlerId, sessionCtx.getSessionId(), e);
//        }
//    }
//
//    @Override
//    public void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg rpcResponse) {
//        log.debug("[{}][{}] 收到服务器RPC响应，请求ID: {}",
//                 handlerId, sessionCtx.getSessionId(), rpcResponse.getRequestId());
//
//        try {
//            // 转换为TCP消息并发送给设备
//            TcpMessage msg = convertToTcpMessage(rpcResponse);
//            sendToDevice(msg);
//        } catch (Exception e) {
//            log.warn("[{}][{}] 处理服务器RPC响应失败", handlerId, sessionCtx.getSessionId(), e);
//        }
//    }
//
//    @Override
//    public void onRemoteSessionCloseCommand(UUID sessionId, TransportProtos.SessionCloseNotificationProto notification) {
//        log.info("[{}][{}] 收到远程会话关闭命令，原因: {}",
//                handlerId, sessionCtx.getSessionId(), notification.getMessage());
//
//        // 发送断开连接消息给设备
//        try {
//            TcpMessage disconnectMsg = TcpMessage.success(TcpMessageType.DISCONNECT, 0);
//            sendToDevice(disconnectMsg);
//
//            // 等待片刻后关闭连接
//            Thread.sleep(100);
//        } catch (Exception e) {
//            log.warn("[{}][{}] 发送断开连接消息失败", handlerId, sessionCtx.getSessionId(), e);
//        } finally {
//            sessionCtx.close();
//        }
//    }
//
//    @Override
//    public void onDeviceProfileUpdate(TransportProtos.SessionInfoProto sessionInfo, DeviceProfile deviceProfile) {
//        log.info("[{}][{}] 设备Profile更新: {}",
//                handlerId, sessionCtx.getSessionId(), deviceProfile.getName());
//        // 可以在此处重新配置会话参数
//    }
//
//    @Override
//    public void onDeviceUpdate(TransportProtos.SessionInfoProto sessionInfo, Device device, Optional<DeviceProfile> deviceProfileOpt) {
//        log.info("[{}][{}] 设备信息更新: {}",
//                handlerId, sessionCtx.getSessionId(), device.getName());
//
//        // 更新会话中的设备信息
//        if (sessionCtx.getDeviceInfo() != null &&
//            sessionCtx.getDeviceInfo().getDeviceId().equals(device.getId())) {
//            // 可以在这里处理设备信息变更
//        }
//    }
//
//    @Override
//    public void onDeviceDeleted(DeviceId deviceId) {
//        log.info("[{}][{}] 设备已被删除: {}",
//                handlerId, sessionCtx.getSessionId(), deviceId);
//
//        // 设备删除，关闭会话
//        sessionCtx.close();
//    }
//
//    /**
//     * 发送消息到设备
//     */
//    private void sendToDevice(TcpMessage message) {
//        if (!sessionCtx.isValid()) {
//            log.warn("[{}][{}] 会话无效，无法发送消息", handlerId, sessionCtx.getSessionId());
//            return;
//        }
//
//        ChannelHandlerContext channel = sessionCtx.getChannel();
//        if (channel == null || !channel.channel().isActive()) {
//            log.warn("[{}][{}] 通道不可用，无法发送消息", handlerId, sessionCtx.getSessionId());
//            return;
//        }
//
//        try {
//            byte[] data = message.encode();
//            channel.writeAndFlush(data);
//
//            if (log.isTraceEnabled()) {
//                log.trace("[{}][{}] 消息已发送，类型: {}, 消息ID: {}, 长度: {}字节",
//                         handlerId, sessionCtx.getSessionId(),
//                         message.getType(), message.getMessageId(), data.length);
//            }
//        } catch (Exception e) {
//            log.error("[{}][{}] 发送消息到设备失败", handlerId, sessionCtx.getSessionId(), e);
//        }
//    }
//
//    /**
//     * 将属性响应转换为TCP消息
//     */
//    private TcpMessage convertToTcpMessage(TransportProtos.GetAttributeResponseMsg msg) {
//        // 这里需要根据你的具体协议实现转换逻辑
//        // 示例：创建属性响应的TCP消息
//        JsonNode payload = JacksonUtil.toJsonNode(msg.toString());
//        return TcpMessage.create(TcpMessageType.ATTRIBUTES, msg.getRequestId(), payload);
//    }
//
//    /**
//     * 将属性更新通知转换为TCP消息
//     */
//    private TcpMessage convertToTcpMessage(TransportProtos.AttributeUpdateNotificationMsg msg) {
//        // 转换为设备可理解的格式
//        JsonNode payload = JacksonUtil.toJsonNode(msg.toString());
//        return TcpMessage.create(TcpMessageType.ATTRIBUTES, sessionCtx.nextMsgId(), payload);
//    }
//
//    /**
//     * 将RPC请求转换为TCP消息
//     */
//    private TcpMessage convertToTcpMessage(TransportProtos.ToDeviceRpcRequestMsg msg) {
//        // 构建RPC请求负载
//        JsonNode payload = JacksonUtil.newObjectNode()
//                .put("method", msg.getMethodName())
//                .put("params", msg.getParams())
//                .put("requestId", msg.getRequestId());
//
//        return TcpMessage.create(TcpMessageType.RPC_REQUEST, msg.getRequestId(), payload);
//    }
//
//    /**
//     * 将服务器RPC响应转换为TCP消息
//     */
//    private TcpMessage convertToTcpMessage(TransportProtos.ToServerRpcResponseMsg msg) {
//        JsonNode payload = JacksonUtil.newObjectNode()
//                .put("requestId", msg.getRequestId())
//                .put("payload", msg.getPayload())
//                .put("error", msg.getError());
//
//        return TcpMessage.create(TcpMessageType.RPC_RESPONSE, msg.getRequestId(), payload);
//    }
//
//    /**
//     * 获取处理器ID
//     */
//    public UUID getHandlerId() {
//        return handlerId;
//    }
//}