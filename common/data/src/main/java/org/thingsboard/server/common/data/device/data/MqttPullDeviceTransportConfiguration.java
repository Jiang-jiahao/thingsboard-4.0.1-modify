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
    /**
     * 可选。仅无人机监管这类「Server Topic 可改」的协议需要：默认 {@code server/chan}，不同防区可改。
     * 大公博创等主题已含 {@code dgb/{deviceid}} 的协议请留空，档案写完整主题。
     */
    private String topicPrefix;
    /**
     * 对方协议中的设备 ID（如大公 {@code deviceid}：W1014_01）。
     * 档案主题 {@code dgb/${device.externalDeviceId}/...} 会替换为本值。不是本平台设备 UUID，也不是设备名称。
     */
    private String externalDeviceId;
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
