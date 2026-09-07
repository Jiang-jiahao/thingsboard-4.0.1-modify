/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.transport.mqtt.MqttPullSubscribeRequest;

import java.util.ArrayList;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MqttPullDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    /** UI 工作模式标记：PULL=平台作为 MQTT 客户端连接外部 Broker */
    private String mqttTransportMode = "PULL";

    private Integer connectTimeoutMs = 10000;
    private Integer keepAliveSec = 60;
    private Boolean cleanSession = true;
    private Long reconnectIntervalMs = 5000L;

    private List<MqttPullSubscribeRequest> subscribeRequests = new ArrayList<>();

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.MQTT_PULL;
    }

    @Override
    public void validate() {
        if (!isValid()) {
            throw new IllegalArgumentException("MQTT pull transport configuration is not valid");
        }
    }

    @JsonIgnore
    public List<MqttPullSubscribeRequest> effectiveSubscribeRequests() {
        if (subscribeRequests == null) {
            return List.of();
        }
        return subscribeRequests.stream().filter(MqttPullSubscribeRequest::isRequestEnabled).toList();
    }

    @JsonIgnore
    private boolean isValid() {
        if (connectTimeoutMs == null || connectTimeoutMs < 0) {
            return false;
        }
        if (keepAliveSec == null || keepAliveSec <= 0) {
            return false;
        }
        if (reconnectIntervalMs == null || reconnectIntervalMs < 1000) {
            return false;
        }
        if (subscribeRequests == null || subscribeRequests.isEmpty()) {
            return false;
        }
        for (MqttPullSubscribeRequest request : subscribeRequests) {
            request.validate();
        }
        return true;
    }
}
