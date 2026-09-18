package org.thingsboard.server.transport.mqtt.rpc;

import lombok.Builder;
import lombok.Value;
import org.thingsboard.server.gen.transport.TransportProtos;

@Value
@Builder
public class PendingMqttServerRpc {
    int requestId;
    TransportProtos.ToDeviceRpcRequestMsg request;
    String responseTopic;
}
