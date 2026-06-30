/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.mqtt;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;

import java.io.Serializable;
import java.util.UUID;

@Data
public class MqttPullSubscribeRequest implements Serializable {

    private String id;
    private String name;
    private Boolean enabled = true;
    private String topic;
    private Integer qos = 1;
    private HttpPullPollDataType dataType = HttpPullPollDataType.TELEMETRY;
    private HttpPullDeviceRoutingConfiguration routing = new HttpPullDeviceRoutingConfiguration();

    @JsonIgnore
    public boolean isEnabled() {
        return enabled == null || enabled;
    }

    @JsonIgnore
    public void validate() {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("MQTT pull subscribe request requires topic");
        }
        if (qos == null || qos < 0 || qos > 2) {
            throw new IllegalArgumentException("MQTT pull subscribe qos must be 0, 1 or 2");
        }
        if (dataType == null) {
            dataType = HttpPullPollDataType.TELEMETRY;
        }
        if (StringUtils.isBlank(id)) {
            id = UUID.randomUUID().toString();
        }
        if (routing != null) {
            routing.validate();
        }
    }
}
