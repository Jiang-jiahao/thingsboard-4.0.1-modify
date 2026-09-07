/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;
import org.thingsboard.server.common.data.transport.mqtt.MqttUplinkTopicMapping;
import org.thingsboard.server.common.data.validation.NoXss;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Data
public class MqttDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    @NoXss
    private String deviceTelemetryTopic = MqttTopics.DEVICE_TELEMETRY_TOPIC;
    @NoXss
    private String deviceAttributesTopic = MqttTopics.DEVICE_ATTRIBUTES_TOPIC;
    @NoXss
    private String deviceAttributesSubscribeTopic = MqttTopics.DEVICE_ATTRIBUTES_TOPIC;

    private List<MqttUplinkTopicMapping> uplinkTopicMappings;

    private TransportPayloadTypeConfiguration transportPayloadTypeConfiguration;
    private boolean sparkplug;
    private Set<String> sparkplugAttributesMetricNames;
    private boolean sendAckOnValidationException;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.MQTT;
    }

    @Override
    public void validate() {
        if (sparkplug) {
            return;
        }
        if (uplinkTopicMappings != null) {
            Set<String> enabledTopics = new HashSet<>();
            for (MqttUplinkTopicMapping mapping : uplinkTopicMappings) {
                mapping.validate();
                if (mapping.isMappingEnabled()) {
                    if (!enabledTopics.add(mapping.getTopic())) {
                        throw new IllegalArgumentException("MQTT uplink topic filters must be unique: " + mapping.getTopic());
                    }
                }
            }
            syncLegacyTopicsFromMappings();
        }
    }

    public TransportPayloadTypeConfiguration getTransportPayloadTypeConfiguration() {
        return Objects.requireNonNullElseGet(transportPayloadTypeConfiguration, JsonTransportPayloadConfiguration::new);
    }

    public String getDeviceTelemetryTopic() {
        return StringUtils.notBlankOrDefault(deviceTelemetryTopic, MqttTopics.DEVICE_TELEMETRY_TOPIC);
    }

    public String getDeviceAttributesTopic() {
        return StringUtils.notBlankOrDefault(deviceAttributesTopic, MqttTopics.DEVICE_ATTRIBUTES_TOPIC);
    }

    public String getDeviceAttributesSubscribeTopic() {
        return StringUtils.notBlankOrDefault(deviceAttributesSubscribeTopic, MqttTopics.DEVICE_ATTRIBUTES_TOPIC);
    }

    /**
     * 运行时使用的上行映射。档案未配置列表时，从旧的遥测/属性主题字段合成两条。
     */
    @JsonIgnore
    public List<MqttUplinkTopicMapping> effectiveUplinkMappings() {
        if (uplinkTopicMappings != null && !uplinkTopicMappings.isEmpty()) {
            return uplinkTopicMappings.stream().filter(MqttUplinkTopicMapping::isMappingEnabled).toList();
        }
        return List.of(
                syntheticMapping("legacy-telemetry", "telemetry", getDeviceTelemetryTopic(), HttpPullPollDataType.TELEMETRY),
                syntheticMapping("legacy-attributes", "attributes", getDeviceAttributesTopic(), HttpPullPollDataType.CLIENT_ATTRIBUTES)
        );
    }

    @JsonIgnore
    public void syncLegacyTopicsFromMappings() {
        if (uplinkTopicMappings == null || uplinkTopicMappings.isEmpty()) {
            return;
        }
        List<MqttUplinkTopicMapping> enabled = uplinkTopicMappings.stream()
                .filter(MqttUplinkTopicMapping::isMappingEnabled)
                .toList();
        if (enabled.isEmpty()) {
            return;
        }
        enabled.stream()
                .filter(m -> m.getDataType() == HttpPullPollDataType.TELEMETRY)
                .findFirst()
                .ifPresent(m -> deviceTelemetryTopic = m.getTopic());
        enabled.stream()
                .filter(m -> m.getDataType() == HttpPullPollDataType.CLIENT_ATTRIBUTES
                        || m.getDataType() == HttpPullPollDataType.SHARED_ATTRIBUTES)
                .findFirst()
                .ifPresent(m -> deviceAttributesTopic = m.getTopic());
    }

    private static MqttUplinkTopicMapping syntheticMapping(String id, String name, String topic, HttpPullPollDataType dataType) {
        MqttUplinkTopicMapping mapping = new MqttUplinkTopicMapping();
        mapping.setId(id);
        mapping.setName(name);
        mapping.setEnabled(true);
        mapping.setTopic(topic);
        mapping.setDataType(dataType);
        return mapping;
    }

}
