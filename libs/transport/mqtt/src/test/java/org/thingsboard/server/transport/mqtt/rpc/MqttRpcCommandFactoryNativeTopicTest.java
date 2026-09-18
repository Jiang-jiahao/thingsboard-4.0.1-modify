package org.thingsboard.server.transport.mqtt.rpc;

import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcBindingType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MqttRpcCommandFactoryNativeTopicTest {

    @Test
    void nativePullIgnoresCustomTopicsAndUsesStandardV1() {
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("setValue");
        method.setBindingType(DeviceProfileRpcBindingType.NATIVE);
        method.setDeviceMethod("setValue");
        method.setMqttRequestTopic("custom/${params.x}/req");
        method.setMqttResponseTopic("custom/${params.x}/resp");

        TransportProtos.ToDeviceRpcRequestMsg request = TransportProtos.ToDeviceRpcRequestMsg.newBuilder()
                .setRequestId(42)
                .setMethodName("setValue")
                .setParams("{\"x\":1}")
                .setOneway(false)
                .build();

        Device device = new Device(new DeviceId(UUID.randomUUID()));
        device.setName("d1");

        MqttRpcCommandFactory.Command cmd = MqttRpcCommandFactory.resolve(method, request, device, null, true);
        assertThat(cmd.isUseStandardNativeTopic()).isFalse();
        assertThat(cmd.getRequestTopic()).isEqualTo("v1/devices/me/rpc/request/42");
        assertThat(cmd.getResponseTopic()).isEqualTo("v1/devices/me/rpc/response/42");
        assertThat(cmd.getPayload()).contains("setValue");
    }

    @Test
    void nativeServerUsesStandardNativeTopicFlag() {
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("setValue");
        method.setBindingType(DeviceProfileRpcBindingType.NATIVE);
        method.setDeviceMethod("setValue");
        method.setMqttRequestTopic("should/be/ignored");

        TransportProtos.ToDeviceRpcRequestMsg request = TransportProtos.ToDeviceRpcRequestMsg.newBuilder()
                .setRequestId(7)
                .setMethodName("setValue")
                .setParams("{}")
                .setOneway(true)
                .build();

        MqttRpcCommandFactory.Command cmd = MqttRpcCommandFactory.resolve(method, request, null, null, false);
        assertThat(cmd.isUseStandardNativeTopic()).isTrue();
        assertThat(cmd.getRequestTopic()).isNull();
    }

    @Test
    void mqttCustomStillUsesConfiguredTopics() {
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("channelOpen");
        method.setBindingType(DeviceProfileRpcBindingType.MQTT_CUSTOM);
        method.setMqttRequestTopic("dgb/${params.deviceId}/request/channel_open");
        method.setMqttResponseTopic("dgb/${params.deviceId}/response/channel_set");
        method.setMqttPayloadTemplate("{\"ok\":true}");

        TransportProtos.ToDeviceRpcRequestMsg request = TransportProtos.ToDeviceRpcRequestMsg.newBuilder()
                .setRequestId(3)
                .setMethodName("channelOpen")
                .setParams("{\"deviceId\":1}")
                .setOneway(false)
                .build();

        MqttRpcCommandFactory.Command cmd = MqttRpcCommandFactory.resolve(method, request, null, null, true);
        assertThat(cmd.getRequestTopic()).isEqualTo("dgb/1/request/channel_open");
        assertThat(cmd.getResponseTopic()).isEqualTo("dgb/1/response/channel_set");
        assertThat(cmd.getPayload()).isEqualTo("{\"ok\":true}");
    }
}
