/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;

/**
 * 设备级 MQTT Pull 配置。
 * <ul>
 *   <li>{@link #collector}：为 true 时该设备作为采集器，连接外部 Broker 并订阅主题。</li>
 *   <li>{@link #externalDeviceId}：多设备路由时，与消息中设备 ID 字段匹配。</li>
 *   <li>{@link #collectorDeviceId}：目标设备归属的采集器设备 ID。</li>
 *   <li>{@link #brokerUrlOverride}：覆盖档案中的 brokerUrl。</li>
 * </ul>
 */
@Data
public class MqttPullDeviceTransportConfiguration implements DeviceTransportConfiguration {

    private Boolean collector = true;
    private String externalDeviceId;
    private String collectorDeviceId;
    private String brokerUrlOverride;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.MQTT_PULL;
    }

    @Override
    public void validate() {
        if (StringUtils.isNotBlank(externalDeviceId)) {
            collector = false;
            brokerUrlOverride = null;
        } else if (StringUtils.isNotBlank(collectorDeviceId)) {
            collector = false;
            brokerUrlOverride = null;
        } else if (Boolean.TRUE.equals(collector)) {
            collectorDeviceId = null;
            externalDeviceId = null;
        }
        if (collector == null) {
            collector = true;
        }
    }

    @JsonIgnore
    public boolean isCollector() {
        if (StringUtils.isNotBlank(externalDeviceId)) {
            return false;
        }
        return !Boolean.FALSE.equals(collector);
    }
}
