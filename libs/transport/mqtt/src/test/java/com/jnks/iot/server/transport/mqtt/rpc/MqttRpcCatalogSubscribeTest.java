package com.jnks.iot.server.transport.mqtt.rpc;

import org.junit.jupiter.api.Test;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileData;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcBindingType;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcMethod;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MqttRpcCatalogSubscribeTest {

    private DeviceProfile profileWithRequestTopic(String topic) {
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("directedStrike");
        method.setBindingType(DeviceProfileRpcBindingType.MQTT_CUSTOM);
        method.setMqttRequestTopic(topic);
        method.setMqttPayloadTemplate("{}");
        DeviceProfileData data = new DeviceProfileData();
        data.setRpcMethods(List.of(method));
        DeviceProfile profile = new DeviceProfile();
        profile.setProfileData(data);
        return profile;
    }

    @Test
    void deviceWildcardSubscriptionCoversParamsPlaceholder() {
        DeviceProfile profile = profileWithRequestTopic(
                "uav-defender/v2/devices/${params.sn}/commands/directed-strike");
        assertThat(MqttRpcCatalog.isCustomRequestSubscribeTopic(profile, null, null,
                "uav-defender/v2/devices/SN-001/commands/#")).isTrue();
    }

    @Test
    void exactCommandSubscriptionIsAccepted() {
        DeviceProfile profile = profileWithRequestTopic(
                "uav-defender/v2/devices/${params.sn}/commands/directed-strike");
        assertThat(MqttRpcCatalog.isCustomRequestSubscribeTopic(profile, null, null,
                "uav-defender/v2/devices/SN-001/commands/directed-strike")).isTrue();
    }

    @Test
    void unrelatedSubscriptionIsRejected() {
        DeviceProfile profile = profileWithRequestTopic(
                "uav-defender/v2/devices/${params.sn}/commands/directed-strike");
        assertThat(MqttRpcCatalog.isCustomRequestSubscribeTopic(profile, null, null,
                "dgb/SN-001/request/#")).isFalse();
        assertThat(MqttRpcCatalog.isCustomRequestSubscribeTopic(profile, null, null,
                "uav-defender/v2/devices/SN-001/responses/#")).isFalse();
    }

    @Test
    void overlapHandlesMultiLevelAndSingleLevelWildcards() {
        assertThat(MqttRpcCatalog.topicFiltersOverlap("a/+/c/d", "a/x/#")).isTrue();
        assertThat(MqttRpcCatalog.topicFiltersOverlap("a/+/c/#", "a/x/c/d")).isTrue();
        assertThat(MqttRpcCatalog.topicFiltersOverlap("a/+/c/d", "a/x/e/#")).isFalse();
        assertThat(MqttRpcCatalog.topicFiltersOverlap("a/+/c/d", "a/x/c")).isFalse();
        assertThat(MqttRpcCatalog.topicFiltersOverlap("a/+", "a/b/c")).isFalse();
        assertThat(MqttRpcCatalog.topicFiltersOverlap("a/+", "a/b")).isTrue();
    }
}
