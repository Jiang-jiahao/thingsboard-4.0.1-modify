//package org.thingsboard.server.transport.tcp.adaptors;
//
//import org.thingsboard.server.common.adaptor.AdaptorException;
//import org.thingsboard.server.gen.transport.TransportProtos;
//import org.thingsboard.server.transport.tcp.session.DeviceSessionCtx;
//import org.thingsboard.server.transport.tcp.util.TcpMessage;
//
//public interface TcpAdaptor {
//
//    // 遥测数据转换
//    TransportProtos.PostTelemetryMsg convertToTelemetry(DeviceSessionCtx ctx,
//                                                      byte[] payload) throws AdaptorException;
//
//    // 属性数据转换
//    TransportProtos.PostAttributeMsg convertToAttributes(DeviceSessionCtx ctx,
//                                                       byte[] payload) throws AdaptorException;
//
//    // RPC响应转换
//    TransportProtos.ToDeviceRpcResponseMsg convertToRpcResponse(DeviceSessionCtx ctx,
//                                                              byte[] payload) throws AdaptorException;
//
//    // 服务器RPC请求转换
//    TransportProtos.ToServerRpcRequestMsg convertToServerRpcRequest(DeviceSessionCtx ctx,
//                                                                  byte[] payload) throws AdaptorException;
//
//    // 转换为TCP消息
//    TcpMessage convertToTcpMessage(TransportProtos.GetAttributeResponseMsg msg);
//    TcpMessage convertToTcpMessage(TransportProtos.AttributeUpdateNotificationMsg msg);
//    TcpMessage convertToTcpMessage(TransportProtos.ToDeviceRpcRequestMsg msg);
//    TcpMessage convertToTcpMessage(TransportProtos.ToServerRpcResponseMsg msg);
//
//    // 创建连接消息
//    TcpMessage createConnectMessage(String token);
//
//    // 创建遥测消息
//    TcpMessage createTelemetryMessage(Object telemetry);
//
//    // 创建属性消息
//    TcpMessage createAttributesMessage(Object attributes);
//
//    // 创建RPC请求消息
//    TcpMessage createRpcRequestMessage(int requestId, String method, Object params);
//
//    // 创建RPC响应消息
//    TcpMessage createRpcResponseMessage(int requestId, Object result);
//}