/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.transport.mqtt.MqttPullAuthConfiguration;

/**
 * 设备级 MQTT Pull 配置：连接地址、Client ID、账号密码均在设备上填写。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MqttPullDeviceTransportConfiguration implements DeviceTransportConfiguration {

    private String brokerUrl;
    private String clientId;
    private MqttPullAuthConfiguration auth = new MqttPullAuthConfiguration();

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.MQTT_PULL;
    }

    @Override
    public void validate() {
        if (StringUtils.isBlank(brokerUrl)) {
            throw new IllegalArgumentException("MQTT pull device requires broker URL");
        }
        if (auth != null) {
            auth.validate();
        }
    }
}
