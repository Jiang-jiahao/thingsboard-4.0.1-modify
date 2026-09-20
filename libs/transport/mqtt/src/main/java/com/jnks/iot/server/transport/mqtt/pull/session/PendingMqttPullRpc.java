package com.jnks.iot.server.transport.mqtt.pull.session;

import lombok.Builder;
import lombok.Value;
import com.jnks.iot.server.gen.transport.TransportProtos;

@Value
@Builder
public class PendingMqttPullRpc {
    int requestId;
    TransportProtos.ToDeviceRpcRequestMsg request;
    TransportProtos.SessionInfoProto sessionInfo;
    String responseTopic;
}
