package com.jnks.iot.server.service.rpc;

import com.google.common.util.concurrent.Futures;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.device.data.DeviceData;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcMethod;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileData;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.BaseAttributeKvEntry;
import com.jnks.iot.server.common.data.kv.StringDataEntry;
import com.jnks.iot.server.common.data.rpc.ToDeviceRpcRequestBody;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequest;
import com.jnks.iot.server.dao.attributes.AttributesService;
import com.jnks.iot.server.dao.device.DeviceProfileService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    void mqttTemplateParamKeysAreCollectedFromTopicsAndPayload() {
        DeviceProfileRpcMethod mqtt = new DeviceProfileRpcMethod();
        mqtt.setId("directedStrike");
        mqtt.setMqttRequestTopic("uav-defender/v2/devices/${params.sn}/commands/directed-strike");
        mqtt.setMqttResponseTopic("uav-defender/v2/devices/${params.sn}/responses/commands");
        mqtt.setMqttPayloadTemplate(
                "{\"sn\":\"${params.sn}\",\"ts\":${params.ts},\"body\":${params.body},\"m\":\"${method}\"}");
        assertThat(DefaultJnksIotCoreDeviceRpcService.mqttTemplateParamKeys(mqtt))
                .containsExactly("sn", "ts", "body");
    }

    @Test
    void mqttMissingParamsAreFilledFromServerAttributes() {
        DeviceProfileRpcMethod mqtt = new DeviceProfileRpcMethod();
        mqtt.setId("directedStrike");
        mqtt.setMqttRequestTopic("uav-defender/v2/devices/${params.sn}/commands/directed-strike");
        mqtt.setMqttPayloadTemplate("{\"sn\":\"${params.sn}\",\"ts\":${params.ts},\"body\":${params.body}}");

        String params = "{\"ts\":1774577533,\"body\":{\"action\":\"ptz_query\"}}";
        assertThat(DefaultJnksIotCoreDeviceRpcService.missingParamKeys(
                params, DefaultJnksIotCoreDeviceRpcService.mqttTemplateParamKeys(mqtt)))
                .containsExactly("sn");

        String merged = DefaultJnksIotCoreDeviceRpcService.fillMissingParams(params, Map.of("sn", "SN-001"));
        assertThat(merged).contains("\"sn\":\"SN-001\"")
                .contains("\"ts\":1774577533")
                .contains("\"action\":\"ptz_query\"");
    }

    @Test
    void mqttCallerProvidedParamIsNotOverridden() {
        String merged = DefaultJnksIotCoreDeviceRpcService.fillMissingParams(
                "{\"sn\":\"CALLER\",\"body\":{}}", Map.of("sn", "ATTR-SN"));
        assertThat(merged).contains("\"sn\":\"CALLER\"").doesNotContain("ATTR-SN");
    }

    @Test
    void mqttEmptyCallerParamsBecomeObject() {
        assertThat(DefaultJnksIotCoreDeviceRpcService.fillMissingParams(null, Map.of("sn", "SN-001")))
                .isEqualTo("{\"sn\":\"SN-001\"}");
    }

    @Test
    void mqttAttributeFillReadsServerScope() {
        AttributesService attributesService = mock(AttributesService.class);
        when(attributesService.find(any(TenantId.class), any(DeviceId.class), eq(AttributeScope.SERVER_SCOPE),
                anyCollection())).thenReturn(Futures.immediateFuture(List.of(
                new BaseAttributeKvEntry(new StringDataEntry("sn", "SN-001"), System.currentTimeMillis()))));
        DefaultJnksIotCoreDeviceRpcService service = new DefaultJnksIotCoreDeviceRpcService(
                null, null, attributesService, null, null, null, null);

        DeviceProfileRpcMethod mqtt = new DeviceProfileRpcMethod();
        mqtt.setId("directedStrike");
        mqtt.setMqttRequestTopic("uav-defender/v2/devices/${params.sn}/commands/directed-strike");
        mqtt.setMqttPayloadTemplate("{\"sn\":\"${params.sn}\",\"ts\":${params.ts},\"body\":${params.body}}");

        TenantId tenantId = TenantId.fromUUID(UUID.randomUUID());
        Device device = new Device(new DeviceId(UUID.randomUUID()));
        device.setTenantId(tenantId);

        String merged = service.mergeMqttCustomParamsFromServerAttributes(tenantId, device, mqtt,
                "{\"ts\":1774577533,\"body\":{\"action\":\"ptz_query\"}}");
        assertThat(merged).contains("\"sn\":\"SN-001\"").contains("\"action\":\"ptz_query\"");
    }

    @Test
    void frameTemplateMqttCustomRequestGetsSnFromServerAttribute() throws Exception {
        DeviceProfileRpcMethod mqtt = new DeviceProfileRpcMethod();
        mqtt.setId("directedStrikePtzQuery");
        mqtt.setBindingType(com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcBindingType.MQTT_CUSTOM);
        mqtt.setMqttRequestTopic("uav-defender/v2/devices/${params.sn}/commands/directed-strike");
        mqtt.setMqttResponseTopic("uav-defender/v2/devices/${params.sn}/responses/commands");
        mqtt.setMqttPayloadTemplate(
                "{\"sn\":\"${params.sn}\",\"ts\":${params.ts},\"body\":${params.body}}");

        DeviceProfileData data = new DeviceProfileData();
        data.setRpcMethods(List.of(mqtt));
        DeviceProfile profile = new DeviceProfile();
        profile.setProfileData(data);

        DeviceProfileService profileService = mock(DeviceProfileService.class);
        when(profileService.findDeviceProfileById(any(TenantId.class), any(DeviceProfileId.class)))
                .thenReturn(profile);
        AttributesService attributesService = mock(AttributesService.class);
        when(attributesService.find(any(TenantId.class), any(DeviceId.class), eq(AttributeScope.SERVER_SCOPE),
                anyCollection())).thenReturn(Futures.immediateFuture(List.of(
                new BaseAttributeKvEntry(new StringDataEntry("sn", "SN-001"), System.currentTimeMillis()))));

        DefaultJnksIotCoreDeviceRpcService service = new DefaultJnksIotCoreDeviceRpcService(
                null, profileService, attributesService, null, null, null, null);

        TenantId tenantId = TenantId.fromUUID(UUID.randomUUID());
        Device device = new Device(new DeviceId(UUID.randomUUID()));
        device.setTenantId(tenantId);
        device.setDeviceProfileId(new DeviceProfileId(UUID.randomUUID()));
        ToDeviceRpcRequest request = new ToDeviceRpcRequest(UUID.randomUUID(), tenantId, device.getId(), false, 0L,
                new ToDeviceRpcRequestBody("directedStrikePtzQuery",
                        "{\"ts\":1774577533,\"body\":{\"action\":\"ptz_query\"}}"),
                false, null, null);

        var frame = DefaultJnksIotCoreDeviceRpcService.class.getDeclaredMethod(
                "frameTemplateRpcIfNeeded", ToDeviceRpcRequest.class, Device.class);
        frame.setAccessible(true);
        ToDeviceRpcRequest effective = (ToDeviceRpcRequest) frame.invoke(service, request, device);

        assertThat(effective).isNotSameAs(request);
        assertThat(effective.getBody().getParams())
                .contains("\"sn\":\"SN-001\"")
                .contains("\"action\":\"ptz_query\"");
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
