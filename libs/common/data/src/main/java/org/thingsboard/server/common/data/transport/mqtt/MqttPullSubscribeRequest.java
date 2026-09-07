/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.mqtt;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;

import java.io.Serializable;
import java.util.UUID;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MqttPullSubscribeRequest implements Serializable {

    private static final String DEFAULT_TELEMETRY_KEY = "mqttPullPayload";

    private String id;
    private String name;
    private Boolean enabled = true;
    private String topic;
    private Integer qos = 1;
    private HttpPullPollDataType dataType = HttpPullPollDataType.TELEMETRY;
    private String telemetryPayloadKey = DEFAULT_TELEMETRY_KEY;

    /** 运行时判断；不能叫 isEnabled()，否则 Jackson 会忽略 enabled 字段导致关闭状态无法保存。 */
    @JsonIgnore
    public boolean isRequestEnabled() {
        return enabled == null || enabled;
    }

    @JsonIgnore
    public String resolveTelemetryPayloadKey() {
        return StringUtils.isNotBlank(telemetryPayloadKey) ? telemetryPayloadKey : DEFAULT_TELEMETRY_KEY;
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
        if (StringUtils.isBlank(telemetryPayloadKey)) {
            telemetryPayloadKey = DEFAULT_TELEMETRY_KEY;
        }
    }
}
