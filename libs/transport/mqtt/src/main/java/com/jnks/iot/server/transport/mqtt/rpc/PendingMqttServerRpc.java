package com.jnks.iot.server.transport.mqtt.rpc;

import lombok.Builder;
import lombok.Value;
import com.jnks.iot.server.gen.transport.TransportProtos;

@Value
@Builder
public class PendingMqttServerRpc {
    int requestId;
    TransportProtos.ToDeviceRpcRequestMsg request;
    String responseTopic;
}
