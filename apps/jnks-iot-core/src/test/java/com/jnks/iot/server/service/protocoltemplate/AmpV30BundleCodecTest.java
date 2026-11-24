package com.jnks.iot.server.service.protocoltemplate;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.device.profile.HexTransportTcpDataConfiguration;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateBundle;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateTransportTcpDataConfiguration;
import com.jnks.iot.server.common.data.device.profile.TcpHexProtocolParser;
import com.jnks.iot.server.common.data.id.TenantId;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 宽频功放数字源 3.0 协议包：下行组帧结果对照文档示例，上行示例帧可解析。
 * <p>
 * 协议包文件随 extras 入库；文件不存在时跳过（例如单独跑子模块时）。
 */
class AmpV30BundleCodecTest {

    private static final Path BUNDLE_PATH =
            Paths.get("../../extras/protocol-templates/zhongke-zhipeng-amp-v30.json");

    private static ProtocolTemplateBundle loadBundle() throws Exception {
        Assumptions.assumeTrue(Files.exists(BUNDLE_PATH), "protocol bundle file not found: " + BUNDLE_PATH);
        ProtocolTemplateBundle bundle = JacksonUtil.fromString(
                Files.readString(BUNDLE_PATH, StandardCharsets.UTF_8), ProtocolTemplateBundle.class);
        assertThat(bundle).isNotNull();
        bundle.setId(UUID.randomUUID().toString());
        return bundle;
    }

    private static ProtocolTemplateHexBuildService buildService(ProtocolTemplateBundle bundle) {
        DefaultProtocolTemplateBundleService bundleService = mock(DefaultProtocolTemplateBundleService.class);
        when(bundleService.findById(any(TenantId.class), any(UUID.class))).thenReturn(Optional.of(bundle));
        return new ProtocolTemplateHexBuildService(bundleService);
    }

    private static String build(ProtocolTemplateHexBuildService service, ProtocolTemplateBundle bundle,
                                long commandValue, String name, Map<String, Object> values) {
        ProtocolTemplateHexBuildRequest request = new ProtocolTemplateHexBuildRequest();
        request.setBundleId(bundle.getId());
        request.setTemplateId("amp_v30_dl");
        request.setCommandValue(commandValue);
        request.setCommandName(name);
        request.setValues(values);
        ProtocolTemplateHexBuildResult result = service.build(TenantId.SYS_TENANT_ID, request);
        assertThat(result.isSuccess()).as(String.valueOf(result.getErrorMessage())).isTrue();
        return result.getHex().toLowerCase();
    }

    @Test
    void buildsDownlinkFramesMatchingDocExamples() throws Exception {
        ProtocolTemplateBundle bundle = loadBundle();
        ProtocolTemplateHexBuildService service = buildService(bundle);

        Map<String, Object> freq = new java.util.HashMap<>();
        freq.put("deviceAddress", 0xFF);
        freq.put("writeMode", 2);
        freq.put("sourceAddr", 3);
        freq.put("startFreqMhz", 2450);
        freq.put("endFreqMhz", 2550);
        assertThat(build(service, bundle, 0x28, "写数字源频率（0x28）", freq))
                .isEqualTo("7e15ff2800060203099209f61f7f");

        assertThat(build(service, bundle, 0x21, "信号源使能控制（0x21）",
                Map.of("deviceAddress", 0xFF, "writeMode", 1, "sourceAddr", 1, "enable", 1)))
                .isEqualTo("7e15ff210003010101c57f");

        assertThat(build(service, bundle, 0x13, "功放状态查询（0x13）", Map.of("deviceAddress", 1)))
                .isEqualTo("7e1501130000d77f");

        assertThat(build(service, bundle, 0x24, "写功放衰减值ATT（0x24）",
                Map.of("deviceAddress", 1, "sourceAddr", 1, "attValue", 5)))
                .isEqualTo("7e1501240003010105bc7f");

        assertThat(build(service, bundle, 0x4B, "云台水平角度定位（0x4B）",
                Map.of("deviceAddress", 1, "angle", 900)))
                .isEqualTo("7e15014b00020384167f");

        assertThat(build(service, bundle, 0x02, "云台水平向右移动（0x02）",
                Map.of("deviceAddress", 1, "hSpeed", 63, "vSpeed", 34)))
                .isEqualTo("7e15010200023f22857f");

        assertThat(build(service, bundle, 0x0A, "通讯心跳包（0x0A）", Map.of("deviceAddress", 0)))
                .isEqualTo("7e15000aff00e27f");

        assertThat(build(service, bundle, 0x2C, "窄带功放开关控制（0x2C）",
                Map.of("deviceAddress", 0xFF, "ampAddr", 1, "enable", 1)))
                .isEqualTo("7e15ff2c000401010101b87f");
    }

    @Test
    void parsesUplinkFramesFromDoc() throws Exception {
        ProtocolTemplateBundle bundle = loadBundle();
        ProtocolTemplateTransportTcpDataConfiguration cfg = new ProtocolTemplateTransportTcpDataConfiguration();
        cfg.setProtocolTemplates(bundle.getProtocolTemplates());
        cfg.setProtocolCommands(bundle.getProtocolCommands());
        HexTransportTcpDataConfiguration hex = cfg.expandToHexTransportTcpDataConfiguration();

        var status = parse(hex, "7E15011300140401041B6ECC03F800DAFF00000000FF00000000927F");
        assertThat(status).isPresent();
        assertThat(status.get().get("cmd").getAsInt()).isEqualTo(0x13);
        assertThat(status.get().get("outputState").getAsInt()).isEqualTo(1);
        assertThat(status.get().get("faultFlags").getAsInt()).isEqualTo(4);
        assertThat(status.get().get("temperature").getAsInt()).isEqualTo(27);
        assertThat(status.get().get("voltageMv").getAsInt()).isEqualTo(28364);
        assertThat(status.get().get("currentMa").getAsInt()).isEqualTo(1016);
        assertThat(status.get().get("powerDbm").getAsDouble()).isCloseTo(21.8, org.assertj.core.data.Offset.offset(1e-6));

        var hAngle = parse(hex, "7E1501590002005A357F");
        assertThat(hAngle).isPresent();
        assertThat(hAngle.get().get("horizontalAngle").getAsDouble()).isCloseTo(0.9, org.assertj.core.data.Offset.offset(1e-6));

        var speed = parse(hex, "7E15017300020807667F");
        assertThat(speed).isPresent();
        assertThat(speed.get().get("hSpeed").getAsInt()).isEqualTo(8);
        assertThat(speed.get().get("vSpeed").getAsInt()).isEqualTo(7);

        var version = parse(hex, "7E15000B00020301DA7F");
        assertThat(version).isPresent();
        assertThat(version.get().get("majorVersion").getAsInt()).isEqualTo(3);
        assertThat(version.get().get("minorVersion").getAsInt()).isEqualTo(1);
    }

    private static Optional<com.google.gson.JsonObject> parse(HexTransportTcpDataConfiguration hex, String frameHex) {
        JsonElement payload = JsonParser.parseString("{\"hex\":\"" + frameHex + "\"}");
        return TcpHexProtocolParser.tryParseUplinkPayloadFromHex(payload, hex.getHexCommandProfiles(),
                        hex.getHexProtocolFields(), hex.getHexLtvRepeating(), hex.getChecksum(), UUID.randomUUID())
                .map(TcpHexProtocolParser.ParsedUplinkPayload::getPayload);
    }
}
