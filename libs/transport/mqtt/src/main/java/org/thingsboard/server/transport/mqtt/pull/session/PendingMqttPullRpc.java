/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull.session;

import lombok.Builder;
import lombok.Value;
import org.thingsboard.server.gen.transport.TransportProtos;

@Value
@Builder
public class PendingMqttPullRpc {
    int requestId;
    TransportProtos.ToDeviceRpcRequestMsg request;
    TransportProtos.SessionInfoProto sessionInfo;
    String responseTopic;
}
