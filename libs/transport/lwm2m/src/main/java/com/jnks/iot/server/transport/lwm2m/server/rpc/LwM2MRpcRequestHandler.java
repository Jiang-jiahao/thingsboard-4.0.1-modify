package com.jnks.iot.server.transport.lwm2m.server.rpc;

import com.jnks.iot.server.gen.transport.TransportProtos;

public interface LwM2MRpcRequestHandler {

    void onToDeviceRpcRequest(TransportProtos.ToDeviceRpcRequestMsg toDeviceRequest, TransportProtos.SessionInfoProto sessionInfo);

    void onToDeviceRpcResponse(TransportProtos.ToDeviceRpcResponseMsg toDeviceRpcResponse, TransportProtos.SessionInfoProto sessionInfo);

    void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg toServerResponse);


}
