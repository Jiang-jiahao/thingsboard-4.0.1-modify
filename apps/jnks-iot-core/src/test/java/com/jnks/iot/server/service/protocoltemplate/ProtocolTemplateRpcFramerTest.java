package com.jnks.iot.server.service.protocoltemplate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileData;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcBindingType;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcMethod;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateBundle;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateCommandDefinition;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateCommandDirection;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateTransportTcpDataConfiguration;
import com.jnks.iot.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.TcpHexValueType;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class ProtocolTemplateRpcFramerTest {

    private static final TenantId TENANT_ID = new TenantId(UUID.fromString("11111111-1111-1111-1111-111111111111"));
    private static final String BUNDLE_ID = "22222222-2222-2222-2222-222222222222";

    @Mock DefaultProtocolTemplateBundleService bundleService;
    @Mock ProtocolTemplateHexBuildService hexBuildService;

    @Test
    void nonTemplateBinding_passesParamsThrough() {
        ProtocolTemplateRpcFramer framer = new ProtocolTemplateRpcFramer(bundleService, hexBuildService);
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("native");
        method.setBindingType(DeviceProfileRpcBindingType.NATIVE);

        assertThat(framer.frameIfNeeded(TENANT_ID, profile(null), method, "{\"a\":1}")).isEqualTo("{\"a\":1}");
    }

    @Test
    void paramsAlreadyContainingHex_passesThrough() {
        ProtocolTemplateRpcFramer framer = new ProtocolTemplateRpcFramer(bundleService, hexBuildService);
        DeviceProfileRpcMethod method = templateMethod();
        String params = "{\"hex\":\"0102\"}";

        assertThat(framer.frameIfNeeded(TENANT_ID, profile(BUNDLE_ID), method, params)).isEqualTo(params);
    }

    @Test
    void tcpTemplateBinding_buildsFrameAndMapsPlatformKeysToTemplateKeys() {
        ProtocolTemplateRpcFramer framer = new ProtocolTemplateRpcFramer(bundleService, hexBuildService);
        DeviceProfileRpcMethod method = templateMethod();
        method.setParamMap(Map.of("power", "pwr"));
        given(bundleService.findById(eq(TENANT_ID), any(UUID.class))).willReturn(Optional.of(bundle(integralCommand())));
        given(hexBuildService.build(eq(TENANT_ID), any())).willReturn(success("aabb"));

        String out = framer.frameIfNeeded(TENANT_ID, profile(BUNDLE_ID), method, "{\"power\":7}");

        assertThat(out).isEqualTo("{\"hex\":\"aabb\"}");
        ProtocolTemplateHexBuildRequest request = capturedBuildRequest();
        assertThat(request.getBundleId()).isEqualTo(BUNDLE_ID);
        assertThat(request.getCommandValue()).isEqualTo(0x10L);
        assertThat(request.getCommandName()).isEqualTo("setPower");
        assertThat(request.getValues()).containsEntry("pwr", 7);
        assertThat(request.getValues()).doesNotContainKey("power");
    }

    @Test
    void byteSliceCommand_usesCommandMatchBytes() {
        ProtocolTemplateRpcFramer framer = new ProtocolTemplateRpcFramer(bundleService, hexBuildService);
        DeviceProfileRpcMethod method = templateMethod();
        ProtocolTemplateCommandDefinition cmd = integralCommand();
        cmd.setMatchValueType(TcpHexValueType.BYTES_AS_HEX);
        cmd.setCommandMatchBytesHex("0102");
        given(bundleService.findById(eq(TENANT_ID), any(UUID.class))).willReturn(Optional.of(bundle(cmd)));
        given(hexBuildService.build(eq(TENANT_ID), any())).willReturn(success("ccdd"));

        framer.frameIfNeeded(TENANT_ID, profile(BUNDLE_ID), method, "{}");

        ProtocolTemplateHexBuildRequest request = capturedBuildRequest();
        assertThat(request.getCommandMatchBytesHex()).isEqualTo("0102");
        assertThat(request.getCommandValue()).isNull();
    }

    @Test
    void missingBundleId_throws() {
        ProtocolTemplateRpcFramer framer = new ProtocolTemplateRpcFramer(bundleService, hexBuildService);

        assertThatThrownBy(() -> framer.frameIfNeeded(TENANT_ID, profile(null), templateMethod(), "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protocolTemplateBundleId");
    }

    @Test
    void buildFailure_throws() {
        ProtocolTemplateRpcFramer framer = new ProtocolTemplateRpcFramer(bundleService, hexBuildService);
        given(bundleService.findById(eq(TENANT_ID), any(UUID.class))).willReturn(Optional.of(bundle(integralCommand())));
        ProtocolTemplateHexBuildResult failed = new ProtocolTemplateHexBuildResult();
        failed.setErrorMessage("bad value");
        given(hexBuildService.build(eq(TENANT_ID), any())).willReturn(failed);

        assertThatThrownBy(() -> framer.frameIfNeeded(TENANT_ID, profile(BUNDLE_ID), templateMethod(), "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bad value");
    }

    private ProtocolTemplateHexBuildRequest capturedBuildRequest() {
        ArgumentCaptor<ProtocolTemplateHexBuildRequest> captor = ArgumentCaptor.forClass(ProtocolTemplateHexBuildRequest.class);
        org.mockito.Mockito.verify(hexBuildService).build(eq(TENANT_ID), captor.capture());
        return captor.getValue();
    }

    private static DeviceProfileRpcMethod templateMethod() {
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("setPower");
        method.setBindingType(DeviceProfileRpcBindingType.TCP_TEMPLATE);
        method.setTemplateId("tpl1");
        method.setTemplateCommandName("setPower");
        method.setCommandValue(0x10L);
        return method;
    }

    private static ProtocolTemplateCommandDefinition integralCommand() {
        ProtocolTemplateCommandDefinition cmd = new ProtocolTemplateCommandDefinition();
        cmd.setTemplateId("tpl1");
        cmd.setName("setPower");
        cmd.setCommandValue(0x10L);
        cmd.setMatchValueType(TcpHexValueType.UINT8);
        cmd.setDirection(ProtocolTemplateCommandDirection.DOWNLINK);
        return cmd;
    }

    private static ProtocolTemplateBundle bundle(ProtocolTemplateCommandDefinition cmd) {
        ProtocolTemplateBundle bundle = new ProtocolTemplateBundle();
        bundle.setId(BUNDLE_ID);
        bundle.setProtocolCommands(List.of(cmd));
        return bundle;
    }

    private static ProtocolTemplateHexBuildResult success(String hex) {
        ProtocolTemplateHexBuildResult result = new ProtocolTemplateHexBuildResult();
        result.setSuccess(true);
        result.setHex(hex);
        return result;
    }

    private static DeviceProfile profile(String bundleId) {
        ProtocolTemplateTransportTcpDataConfiguration data = new ProtocolTemplateTransportTcpDataConfiguration();
        data.setProtocolTemplateBundleId(bundleId);
        TcpDeviceProfileTransportConfiguration tcp = new TcpDeviceProfileTransportConfiguration();
        tcp.setTransportTcpDataTypeConfiguration(data);
        DeviceProfileData profileData = new DeviceProfileData();
        profileData.setTransportConfiguration(tcp);
        DeviceProfile profile = new DeviceProfile();
        profile.setProfileData(profileData);
        return profile;
    }
}
