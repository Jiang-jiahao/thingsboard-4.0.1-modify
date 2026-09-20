package com.jnks.iot.server.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.TbResource;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.device.data.DefaultDeviceConfiguration;
import com.jnks.iot.server.common.data.device.data.DefaultDeviceTransportConfiguration;
import com.jnks.iot.server.common.data.device.data.DeviceConfiguration;
import com.jnks.iot.server.common.data.device.data.DeviceTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileData;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.AttributeKey;
import com.jnks.iot.server.common.data.kv.BaseAttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BooleanDataEntry;
import com.jnks.iot.server.common.data.kv.DoubleDataEntry;
import com.jnks.iot.server.common.data.kv.JsonDataEntry;
import com.jnks.iot.server.common.data.kv.StringDataEntry;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.data.rpc.RpcError;
import com.jnks.iot.server.common.data.rpc.ToDeviceRpcRequestBody;
import com.jnks.iot.server.common.data.security.DeviceCredentials;
import com.jnks.iot.server.common.data.security.DeviceCredentialsType;
import com.jnks.iot.server.common.data.sync.vc.RepositorySettings;
import com.jnks.iot.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import com.jnks.iot.server.common.data.tenant.profile.TenantProfileConfiguration;
import com.jnks.iot.server.common.msg.plugin.ComponentLifecycleMsg;
import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponse;
import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponseActorMsg;
import com.jnks.iot.server.common.msg.rpc.RemoveRpcActorMsg;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequest;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequestActorMsg;
import com.jnks.iot.server.common.msg.rule.engine.DeviceAttributesEventNotificationMsg;
import com.jnks.iot.server.common.msg.rule.engine.DeviceCredentialsUpdateNotificationMsg;
import com.jnks.iot.server.common.msg.rule.engine.DeviceNameOrTypeUpdateMsg;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ProtoUtilsTest {

    TenantId tenantId = TenantId.fromUUID(UUID.fromString("35e10f77-16e7-424d-ae46-ee780f87ac4f"));
    EntityId entityId = new RuleChainId(UUID.fromString("c640b635-4f0f-41e6-b10b-25a86003094e"));
    DeviceId deviceId = new DeviceId(UUID.fromString("ceebb9e5-4239-437c-a507-dc5f71f1232d"));
    UUID id = UUID.fromString("31a07d85-6ed5-46f8-83c0-6715cb0a8782");
    static EasyRandom easyRandom;

    @BeforeAll
    static void init() {
        EasyRandomParameters parameters = new EasyRandomParameters()
                .randomize(DeviceConfiguration.class, DefaultDeviceConfiguration::new)
                .randomize(DeviceTransportConfiguration.class, DefaultDeviceTransportConfiguration::new)
                .randomize(JsonNode.class, JacksonUtil::newObjectNode)
                .randomize(DeviceProfileData.class, DeviceProfileData::new)
                .randomize(TenantProfileConfiguration.class, DefaultTenantProfileConfiguration::new)
                .randomize(EntityId.class, () -> new DeviceId(UUID.randomUUID()));
        easyRandom = new EasyRandom(parameters);
    }

    @Test
    void protoComponentLifecycleSerialization() {
        ComponentLifecycleMsg msg = new ComponentLifecycleMsg(tenantId, entityId, ComponentLifecycleEvent.UPDATED);
        assertThat(ProtoUtils.fromProto(ProtoUtils.toProto(msg))).as("deserialized").isEqualTo(msg);
        msg = new ComponentLifecycleMsg(tenantId, entityId, ComponentLifecycleEvent.STARTED);
        assertThat(ProtoUtils.fromProto(ProtoUtils.toProto(msg))).as("deserialized").isEqualTo(msg);
    }

    @Test
    void protoEntityTypeSerialization() {
        for (EntityType entityType : EntityType.values()) {
            TransportProtos.EntityTypeProto proto = ProtoUtils.toProto(entityType);
            if (proto == null) {
                continue;
            }
            assertThat(ProtoUtils.fromProto(proto)).as(entityType.getNormalName()).isEqualTo(entityType);
        }
    }

    @Test
    void protoDeviceNameOrTypeSerialization() {
        String deviceName = "test", deviceType = "test";
        DeviceNameOrTypeUpdateMsg msg = new DeviceNameOrTypeUpdateMsg(tenantId, deviceId, deviceName, deviceType);
        TransportProtos.ToDeviceActorNotificationMsgProto serializedMsg = ProtoUtils.toProto(msg);
        Assertions.assertNotNull(serializedMsg);
        assertThat(ProtoUtils.fromProto(serializedMsg)).as("deserialized").isEqualTo(msg);
    }

    @Test
    void protoDeviceAttributesEventSerialization() {
        DeviceAttributesEventNotificationMsg msg = new DeviceAttributesEventNotificationMsg(tenantId, deviceId, null, "CLIENT_SCOPE",
                List.of(new BaseAttributeKvEntry(System.currentTimeMillis(), new StringDataEntry("key", "value"))), false);
        TransportProtos.ToDeviceActorNotificationMsgProto serializedMsg = ProtoUtils.toProto(msg);
        Assertions.assertNotNull(serializedMsg);
        assertThat(ProtoUtils.fromProto(serializedMsg)).as("deserialized").isEqualTo(msg);

        msg = new DeviceAttributesEventNotificationMsg(tenantId, deviceId, null, "SERVER_SCOPE",
                List.of(new BaseAttributeKvEntry(System.currentTimeMillis(), new DoubleDataEntry("doubleEntry", 231.5)),
                        new BaseAttributeKvEntry(System.currentTimeMillis(), new JsonDataEntry("jsonEntry", "jsonValue"))), false);
        serializedMsg = ProtoUtils.toProto(msg);
        Assertions.assertNotNull(serializedMsg);
        assertThat(ProtoUtils.fromProto(serializedMsg)).as("deserialized").isEqualTo(msg);

        msg = new DeviceAttributesEventNotificationMsg(tenantId, deviceId, null, "SERVER_SCOPE",
                List.of(new BaseAttributeKvEntry(System.currentTimeMillis(), new DoubleDataEntry("entry", 11.3)),
                        new BaseAttributeKvEntry(System.currentTimeMillis(), new BooleanDataEntry("jsonEntry", true))), false);
        serializedMsg = ProtoUtils.toProto(msg);
        Assertions.assertNotNull(serializedMsg);
        assertThat(ProtoUtils.fromProto(serializedMsg)).as("deserialized").isEqualTo(msg);

        msg = new DeviceAttributesEventNotificationMsg(tenantId, deviceId, Set.of(new AttributeKey("SHARED_SCOPE", "attributeKey")), null, null, true);
        serializedMsg = ProtoUtils.toProto(msg);
        Assertions.assertNotNull(serializedMsg);
        assertThat(ProtoUtils.fromProto(serializedMsg)).as("deserialized").isEqualTo(msg);
    }

    @Test
    void protoDeviceCredentialsUpdateSerialization() {
        DeviceCredentials deviceCredentials = new DeviceCredentials();
        deviceCredentials.setDeviceId(deviceId);
        deviceCredentials.setCredentialsType(DeviceCredentialsType.ACCESS_TOKEN);
        deviceCredentials.setCredentialsValue("test");
        deviceCredentials.setCredentialsId("test");
        DeviceCredentialsUpdateNotificationMsg msg = new DeviceCredentialsUpdateNotificationMsg(tenantId, deviceId, deviceCredentials);
        TransportProtos.ToDeviceActorNotificationMsgProto serializedMsg = ProtoUtils.toProto(msg);
        Assertions.assertNotNull(serializedMsg);
        assertThat(ProtoUtils.fromProto(serializedMsg)).as("deserialized").isEqualTo(msg);
    }

    @Test
    void protoToDeviceRpcRequestSerialization() {
        String serviceId = "cadcaac6-85c3-4211-9756-f074dcd1e7f7";
        ToDeviceRpcRequest request = new ToDeviceRpcRequest(id, tenantId, deviceId, true, 0, new ToDeviceRpcRequestBody("method", "params"), false, 0, "");
        ToDeviceRpcRequestActorMsg msg = new ToDeviceRpcRequestActorMsg(serviceId, request);
        TransportProtos.ToDeviceActorNotificationMsgProto serializedMsg = ProtoUtils.toProto(msg);
        Assertions.assertNotNull(serializedMsg);
        assertThat(ProtoUtils.fromProto(serializedMsg)).as("deserialized").isEqualTo(msg);
    }

    @Test
    void protoFromDeviceRpcResponseSerialization() {
        FromDeviceRpcResponseActorMsg msg = new FromDeviceRpcResponseActorMsg(23, tenantId, deviceId, new FromDeviceRpcResponse(id, "response", RpcError.NOT_FOUND));
        TransportProtos.ToDeviceActorNotificationMsgProto serializedMsg = ProtoUtils.toProto(msg);
        Assertions.assertNotNull(serializedMsg);
        assertThat(ProtoUtils.fromProto(serializedMsg)).as("deserialized").isEqualTo(msg);
    }

    @Test
    void protoRemoveRpcActorSerialization() {
        RemoveRpcActorMsg msg = new RemoveRpcActorMsg(tenantId, deviceId, id);
        TransportProtos.ToDeviceActorNotificationMsgProto serializedMsg = ProtoUtils.toProto(msg);
        Assertions.assertNotNull(serializedMsg);
        assertThat(ProtoUtils.fromProto(serializedMsg)).as("deserialized").isEqualTo(msg);
    }

    private static final String description = "Failed to deserialize %s, because found some new fields which absent in %sProto!!!";

    @Test
    void protoSerializationDeserializationEntities() {
        Device expectedDevice = easyRandom.nextObject(Device.class);
        TransportProtos.DeviceProto deviceProto = ProtoUtils.toProto(expectedDevice);
        Device actualDevice = ProtoUtils.fromProto(deviceProto);
        assertEqualDeserializedEntity(expectedDevice, actualDevice, "Device");

        DeviceCredentials expectedCredentials = easyRandom.nextObject(DeviceCredentials.class);
        TransportProtos.DeviceCredentialsProto credentialsProto = ProtoUtils.toProto(expectedCredentials);
        DeviceCredentials actualCredentials = ProtoUtils.fromProto(credentialsProto);
        assertEqualDeserializedEntity(expectedCredentials, actualCredentials, "DeviceCredentials");

        DeviceProfile expectedDeviceProfile = easyRandom.nextObject(DeviceProfile.class);
        TransportProtos.DeviceProfileProto deviceProfileProto = ProtoUtils.toProto(expectedDeviceProfile);
        DeviceProfile actualDeviceProfile = ProtoUtils.fromProto(deviceProfileProto);
        assertEqualDeserializedEntity(expectedDeviceProfile, actualDeviceProfile, "DeviceProfile");

        Tenant expectedTenant = easyRandom.nextObject(Tenant.class);
        TransportProtos.TenantProto tenantProto = ProtoUtils.toProto(expectedTenant);
        Tenant actualTenant = ProtoUtils.fromProto(tenantProto);
        assertEqualDeserializedEntity(expectedTenant, actualTenant, "Tenant");

        TenantProfile expectedTenantProfile = easyRandom.nextObject(TenantProfile.class);
        TransportProtos.TenantProfileProto tenantProfileProto = ProtoUtils.toProto(expectedTenantProfile);
        TenantProfile actualTenantProfile = ProtoUtils.fromProto(tenantProfileProto);
        assertEqualDeserializedEntity(expectedTenantProfile, actualTenantProfile, "TenantProfile");

        TbResource expectedResource = easyRandom.nextObject(TbResource.class);
        TransportProtos.TbResourceProto resourceProto = ProtoUtils.toProto(expectedResource);
        TbResource actualResource = ProtoUtils.fromProto(resourceProto);
        assertEqualDeserializedEntity(expectedResource, actualResource, "TbResource");

        ApiUsageState expectedState = easyRandom.nextObject(ApiUsageState.class);
        TransportProtos.ApiUsageStateProto stateProto = ProtoUtils.toProto(expectedState);
        ApiUsageState actualState = ProtoUtils.fromProto(stateProto);
        assertEqualDeserializedEntity(expectedState, actualState, "ApiUsageState");

        RepositorySettings expectedSettings = easyRandom.nextObject(RepositorySettings.class);
        TransportProtos.RepositorySettingsProto settingsProto = ProtoUtils.toProto(expectedSettings);
        RepositorySettings actualSettings = ProtoUtils.fromProto(settingsProto);
        assertEqualDeserializedEntity(expectedSettings, actualSettings, "RepositorySettings");
    }

    private void assertEqualDeserializedEntity(Object expected, Object actual, String entityName) {
        assertThat(actual).as(String.format(description, entityName, entityName)).isEqualTo(expected);
    }

}
