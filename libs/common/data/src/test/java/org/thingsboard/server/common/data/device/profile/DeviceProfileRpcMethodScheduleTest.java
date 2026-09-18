package org.thingsboard.server.common.data.device.profile;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceProfileRpcMethodScheduleTest {

    @Test
    void scheduleActiveOnlyForHttpOutboundWithValidInterval() {
        DeviceProfileRpcMethod method = baseHttpOutbound();
        method.setScheduleEnabled(true);
        method.setScheduleIntervalMs(5000L);
        assertThat(method.isScheduleActive()).isTrue();
        method.validate(null);
    }

    @Test
    void scheduleRequiresMinInterval() {
        // 定时配置已迁到设备级 deviceData.scheduledRpcs；档案上这两个字段为 @Deprecated 且不再参与校验
        // （间隔 >= 1000 的校验由 DeviceScheduledRpc.validate() 负责，见 C.1 用例）。
        DeviceProfileRpcMethod method = baseHttpOutbound();
        method.setScheduleEnabled(true);
        method.setScheduleIntervalMs(500L);
        assertThat(method.isScheduleActive()).isFalse();
        method.validate(null);
    }

    @Test
    void scheduleAllowedForNativeBinding() {
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("nativePing");
        method.setBindingType(DeviceProfileRpcBindingType.NATIVE);
        method.setDeviceMethod("ping");
        method.setScheduleEnabled(true);
        method.setScheduleIntervalMs(5000L);
        assertThat(method.isScheduleActive()).isTrue();
        method.validate(null);
    }

    private static DeviceProfileRpcMethod baseHttpOutbound() {
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("httpSetValue");
        method.setBindingType(DeviceProfileRpcBindingType.HTTP_OUTBOUND);
        method.setHttpUrl("http://127.0.0.1/rpc");
        method.setHttpMethod("POST");
        return method;
    }
}
