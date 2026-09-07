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

/**
 * MQTT 服务端（设备连本平台 Broker）上行主题映射：设备 PUBLISH 的主题过滤器及数据落点。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MqttUplinkTopicMapping implements Serializable {

    private String id;
    private String name;
    private Boolean enabled = true;
    private String topic;
    private HttpPullPollDataType dataType = HttpPullPollDataType.TELEMETRY;
    /**
     * 仅 TELEMETRY：留空表示按平台原生遥测 JSON 解析；有值则把整段负载包在该键下。
     */
    private String telemetryPayloadKey;

    @JsonIgnore
    public boolean isMappingEnabled() {
        return enabled == null || enabled;
    }

    @JsonIgnore
    public boolean hasTelemetryPayloadKey() {
        return StringUtils.isNotBlank(telemetryPayloadKey);
    }

    @JsonIgnore
    public void validate() {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("MQTT uplink topic mapping requires topic");
        }
        if (dataType == null) {
            dataType = HttpPullPollDataType.TELEMETRY;
        }
        if (StringUtils.isBlank(id)) {
            id = UUID.randomUUID().toString();
        }
    }
}
