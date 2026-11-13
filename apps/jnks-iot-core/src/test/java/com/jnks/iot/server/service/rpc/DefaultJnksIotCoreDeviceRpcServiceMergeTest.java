package com.jnks.iot.server.service.rpc;

import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.device.data.DeviceData;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultJnksIotCoreDeviceRpcServiceMergeTest {

    private static DeviceProfileRpcMethod method(String templateJson) {
        DeviceProfileRpcMethod m = new DeviceProfileRpcMethod();
        m.setId("powerOn");
        m.setParamsTemplateJson(templateJson);
        return m;
    }

    private static Device deviceWithDefaults(Map<String, Map<String, Object>> byMethod) {
        DeviceData data = new DeviceData();
        data.setRpcParamDefaultsByMethod(byMethod);
        Device device = new Device();
        device.setDeviceData(data);
        return device;
    }

    @Test
    void callerWithoutParamsGetsTemplateHex() {
        Device device = deviceWithDefaults(null);
        String merged = DefaultJnksIotCoreDeviceRpcService.mergeNativeRpcParams(
                device, method("{\"hex\":\"2443\"}"), null);
        assertThat(merged).isEqualTo("{\"hex\":\"2443\"}");
    }

    @Test
    void deviceDefaultsAndCallerOverridesAreMerged() {
        Device device = deviceWithDefaults(Map.of("powerOn", Map.of("deviceId", 1, "hex", "AA")));
        String merged = DefaultJnksIotCoreDeviceRpcService.mergeNativeRpcParams(
                device, method("{\"hex\":\"2443\"}"), "{\"hex\":\"BB\"}");
        assertThat(merged).contains("\"deviceId\":1").contains("\"hex\":\"BB\"");
    }

    @Test
    void noDefaultsKeepsCallerParams() {
        String merged = DefaultJnksIotCoreDeviceRpcService.mergeNativeRpcParams(
                deviceWithDefaults(null), method(null), "{\"a\":1}");
        assertThat(merged).isEqualTo("{\"a\":1}");
    }

    @Test
    void templateRpcMergesDeviceDefaultsAndCallerWins() {
        DeviceProfileRpcMethod ptzPan = method("{\"hex\":\"should-not-be-used\"}");
        ptzPan.setId("ptzPan");
        Device device = deviceWithDefaults(Map.of("ptzPan", Map.of("deviceId", 510010, "moduleType", 5)));
        String merged = DefaultJnksIotCoreDeviceRpcService.mergeRpcParams(
                device, ptzPan, "{\"angle\":90}", false);
        assertThat(merged)
                .contains("\"deviceId\":510010")
                .contains("\"moduleType\":5")
                .contains("\"angle\":90")
                .doesNotContain("should-not-be-used");
    }

    @Test
    void legacyDefaultsOnlyApplyWhenNoPerMethodEntryExists() {
        DeviceData data = new DeviceData();
        data.setRpcParamDefaultsByMethod(Map.of("powerOn", Map.of("deviceId", 1)));
        data.setRpcParamDefaults(Map.of("deviceId", 99, "legacyOnly", true));
        Device device = new Device();
        device.setDeviceData(data);

        String merged = DefaultJnksIotCoreDeviceRpcService.mergeNativeRpcParams(device, method(null), "{}");
        assertThat(merged).contains("\"deviceId\":1").doesNotContain("legacyOnly");

        DeviceProfileRpcMethod other = new DeviceProfileRpcMethod();
        other.setId("powerOff");
        String fallback = DefaultJnksIotCoreDeviceRpcService.mergeNativeRpcParams(device, other, "{}");
        assertThat(fallback).doesNotContain("legacyOnly");
    }

    @Test
    void customPayloadMergesTemplateAndDeviceDefaults() {
        DeviceProfileRpcMethod query = method("{\"msgtype\":\"query\",\"eid\":\"0\"}");
        query.setId("query");
        Device device = deviceWithDefaults(Map.of("query", Map.of("eid", 0, "msgtype", "query")));
        String merged = DefaultJnksIotCoreDeviceRpcService.mergeCustomPayloadParams(device, query, "{}");
        assertThat(merged).contains("\"msgtype\":\"query\"").contains("\"eid\":0");
    }

    @Test
    void customPayloadCallerOverridesDeviceDefaults() {
        DeviceProfileRpcMethod query = method("{\"msgtype\":\"query\",\"eid\":\"0\"}");
        query.setId("query");
        Device device = deviceWithDefaults(Map.of("query", Map.of("eid", 0)));
        String merged = DefaultJnksIotCoreDeviceRpcService.mergeCustomPayloadParams(device, query, "{\"eid\":\"150101001\"}");
        assertThat(merged).contains("\"eid\":\"150101001\"");
    }

    @Test
    void customPayloadRawTextTemplateFallsBackToTemplate() {
        DeviceProfileRpcMethod raw = method("$C1100000001000*");
        raw.setId("powerOn");
        Device device = deviceWithDefaults(null);
        assertThat(DefaultJnksIotCoreDeviceRpcService.mergeCustomPayloadParams(device, raw, null))
                .isEqualTo("$C1100000001000*");
        assertThat(DefaultJnksIotCoreDeviceRpcService.mergeCustomPayloadParams(device, raw, "$C2*"))
                .isEqualTo("$C2*");
    }

    @Test
    void nonObjectCallerParamsArePassedThrough() {
        Device device = deviceWithDefaults(Map.of("powerOn", Map.of("deviceId", 1)));
        String stringParams = "plain-string";
        assertThat(DefaultJnksIotCoreDeviceRpcService.mergeNativeRpcParams(
                device, method("{\"hex\":\"2443\"}"), stringParams)).isEqualTo(stringParams);
        assertThat(DefaultJnksIotCoreDeviceRpcService.mergeRpcParams(
                device, method("{\"hex\":\"2443\"}"), "[1,2]", false)).isEqualTo("[1,2]");
    }
}
