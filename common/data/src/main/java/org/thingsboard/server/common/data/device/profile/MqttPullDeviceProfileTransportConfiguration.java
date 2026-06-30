/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullRoutingMode;
import org.thingsboard.server.common.data.transport.mqtt.MqttPullAuthConfiguration;
import org.thingsboard.server.common.data.transport.mqtt.MqttPullSubscribeRequest;

import java.util.ArrayList;
import java.util.List;

@Data
public class MqttPullDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    /** UI 工作模式标记：PULL=平台作为 MQTT 客户端连接外部 Broker */
    private String mqttTransportMode = "PULL";

    private String brokerUrl;
    private Integer connectTimeoutMs = 10000;
    private Integer keepAliveSec = 60;
    private Boolean cleanSession = true;
    private Long reconnectIntervalMs = 5000L;
    private String clientIdPrefix;

    private List<MqttPullSubscribeRequest> subscribeRequests = new ArrayList<>();
    private MqttPullAuthConfiguration auth = new MqttPullAuthConfiguration();

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
        return subscribeRequests.stream().filter(MqttPullSubscribeRequest::isEnabled).toList();
    }

    @JsonIgnore
    public HttpPullDeviceRoutingConfiguration resolveRouting(MqttPullSubscribeRequest request) {
        return request.getRouting();
    }

    @JsonIgnore
    public boolean needsMultiDeviceTargets() {
        for (MqttPullSubscribeRequest request : effectiveSubscribeRequests()) {
            HttpPullDeviceRoutingConfiguration r = resolveRouting(request);
            if (r != null && (r.getRoutingMode() == HttpPullRoutingMode.MULTI_DEVICE
                    || r.getRoutingMode() == HttpPullRoutingMode.PER_MESSAGE
                    || r.getRoutingMode() == HttpPullRoutingMode.AUTO)) {
                return true;
            }
        }
        return false;
    }

    @JsonIgnore
    private boolean isValid() {
        if (StringUtils.isBlank(brokerUrl)) {
            return false;
        }
        if (connectTimeoutMs == null || connectTimeoutMs < 0) {
            return false;
        }
        if (keepAliveSec == null || keepAliveSec <= 0) {
            return false;
        }
        if (reconnectIntervalMs == null || reconnectIntervalMs < 1000) {
            return false;
        }
        List<MqttPullSubscribeRequest> requests = effectiveSubscribeRequests();
        if (requests.isEmpty()) {
            return false;
        }
        for (MqttPullSubscribeRequest request : requests) {
            request.validate();
        }
        if (auth != null) {
            auth.validate();
        }
        return true;
    }
}
