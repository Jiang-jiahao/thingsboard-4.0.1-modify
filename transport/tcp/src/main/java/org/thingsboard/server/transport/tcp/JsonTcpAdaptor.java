//package org.thingsboard.server.transport.tcp;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import lombok.extern.slf4j.Slf4j;
//import org.thingsboard.common.util.JacksonUtil;
//import org.thingsboard.server.common.adaptor.AdaptorException;
//import org.thingsboard.server.common.adaptor.JsonConverter;
//import org.thingsboard.server.gen.transport.TransportProtos;
//import org.thingsboard.server.transport.tcp.adaptors.TcpAdaptor;
//import org.thingsboard.server.transport.tcp.session.DeviceSessionCtx;
//import org.thingsboard.server.transport.tcp.util.TcpMessage;
//import org.thingsboard.server.transport.tcp.util.TcpMessageType;
//
//@Slf4j
//public class JsonTcpAdaptor implements TcpAdaptor {
//
//    @Override
//    public TransportProtos.PostTelemetryMsg convertToTelemetry(DeviceSessionCtx ctx,
//                                                             byte[] payload) throws AdaptorException {
//        try {
//            JsonNode jsonNode = JacksonUtil.fromBytes(payload, JsonNode.class);
//            return JsonConverter.convertToTelemetryProto(jsonNode);
//        } catch (Exception e) {
//            throw new AdaptorException(e);
//        }
//    }
//
//    @Override
//    public TransportProtos.PostAttributeMsg convertToAttributes(DeviceSessionCtx ctx,
//                                                              byte[] payload) throws AdaptorException {
//        try {
//            JsonNode jsonNode = JacksonUtil.fromBytes(payload, JsonNode.class);
//            return JsonConverter.convertToAttributesProto(jsonNode);
//        } catch (Exception e) {
//            throw new AdaptorException(e);
//        }
//    }
//
//    @Override
//    public TransportProtos.ToDeviceRpcResponseMsg convertToRpcResponse(DeviceSessionCtx ctx,
//                                                                     byte[] payload) throws AdaptorException {
//        try {
//            String jsonStr = new String(payload);
//            return TransportProtos.ToDeviceRpcResponseMsg.newBuilder()
//                .setPayload(jsonStr)
//                .build();
//        } catch (Exception e) {
//            throw new AdaptorException(e);
//        }
//    }
//
//    @Override
//    public TransportProtos.ToServerRpcRequestMsg convertToServerRpcRequest(DeviceSessionCtx ctx,
//                                                                         byte[] payload) throws AdaptorException {
//        try {
//            JsonNode jsonNode = JacksonUtil.fromBytes(payload, JsonNode.class);
//            return TransportProtos.ToServerRpcRequestMsg.newBuilder()
//                .setMethodName(jsonNode.get("method").asText())
//                .setParams(jsonNode.get("params").toString())
//                .build();
//        } catch (Exception e) {
//            throw new AdaptorException(e);
//        }
//    }
//
//    @Override
//    public TcpMessage convertToTcpMessage(TransportProtos.GetAttributeResponseMsg msg) {
//        String json = JsonConverter.toJson(msg).toString();
//        return TcpMessage.create(TcpMessageType.ATTRIBUTES, 0, json);
//    }
//
//    @Override
//    public TcpMessage convertToTcpMessage(TransportProtos.AttributeUpdateNotificationMsg msg) {
//        String json = JsonConverter.toJson(msg).toString();
//        return TcpMessage.create(TcpMessageType.ATTRIBUTES, 0, json);
//    }
//
//    @Override
//    public TcpMessage convertToTcpMessage(TransportProtos.ToDeviceRpcRequestMsg msg) {
//        String json = JsonConverter.toJson(msg, true).toString();
//        return TcpMessage.create(TcpMessageType.RPC_REQUEST, msg.getRequestId(), json);
//    }
//
//    @Override
//    public TcpMessage convertToTcpMessage(TransportProtos.ToServerRpcResponseMsg msg) {
//        String json = JsonConverter.toJson(msg).toString();
//        return TcpMessage.create(TcpMessageType.RPC_RESPONSE, msg.getRequestId(), json);
//    }
//
//    @Override
//    public TcpMessage createConnectMessage(String token) {
//        return TcpMessage.create(TcpMessageType.CONNECT, 0,
//            JacksonUtil.newObjectNode().put("token", token));
//    }
//
//    @Override
//    public TcpMessage createTelemetryMessage(Object telemetry) {
//        return TcpMessage.create(TcpMessageType.TELEMETRY, 0, telemetry);
//    }
//
//    @Override
//    public TcpMessage createAttributesMessage(Object attributes) {
//        return TcpMessage.create(TcpMessageType.ATTRIBUTES, 0, attributes);
//    }
//
//    @Override
//    public TcpMessage createRpcRequestMessage(int requestId, String method, Object params) {
//        return TcpMessage.create(TcpMessageType.RPC_REQUEST, requestId,
//            JacksonUtil.newObjectNode()
//                .put("method", method)
//                .set("params", JacksonUtil.valueToTree(params)));
//    }
//
//    @Override
//    public TcpMessage createRpcResponseMessage(int requestId, Object result) {
//        return TcpMessage.create(TcpMessageType.RPC_RESPONSE, requestId, result);
//    }
//}