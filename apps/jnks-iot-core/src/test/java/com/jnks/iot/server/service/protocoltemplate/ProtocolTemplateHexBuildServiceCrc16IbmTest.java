package com.jnks.iot.server.service.protocoltemplate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateBundle;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateCommandDefinition;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateCommandDirection;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateDefinition;
import com.jnks.iot.server.common.data.device.profile.TcpHexChecksumDefinition;
import com.jnks.iot.server.common.data.device.profile.TcpHexFieldDefinition;
import com.jnks.iot.server.common.data.device.profile.TcpHexValueType;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;

/**
 * 主从模块协议下行：参长自动、命令级 CRC-16/IBM、帧尾 0x7E。
 * 期望值取自《主从模块通讯协议》V1.1.5 的命令样例。
 */
@ExtendWith(MockitoExtension.class)
class ProtocolTemplateHexBuildServiceCrc16IbmTest {

    private static final TenantId TENANT_ID = new TenantId(UUID.fromString("11111111-1111-1111-1111-111111111111"));
    private static final UUID BUNDLE_ID = UUID.fromString("33333333-3333-3333-3333-333333333333");
    private static final String TEMPLATE_ID = "lingxin_master_slave_7e_dl";

    @Mock DefaultProtocolTemplateBundleService bundleService;

    @Test
    void buildsQueryFrameWithoutData() {
        ProtocolTemplateHexBuildService service = serviceWith(command(0x21, "0x21 设备状态查询", 7));
        ProtocolTemplateHexBuildResult result = build(service, 0x21, "0x21 设备状态查询",
                Map.of("destAddress", 8));
        assertThat(result.getHex()).isEqualTo("7e080321412a7e");
    }

    @Test
    void buildsPowerLevelFrameWithChecksumBeforeTrailingDelimiter() {
        ProtocolTemplateHexBuildService service = serviceWith(command(0x29, "0x29 等级功放设置", 10));
        ProtocolTemplateHexBuildResult result = build(service, 0x29, "0x29 等级功放设置",
                Map.of("destAddress", 2, "powerLevel", 1, "powerValue", 1500));
        assertThat(result.getHex()).isEqualTo("7e02062901dc0548bd7e");
    }

    private ProtocolTemplateHexBuildResult build(ProtocolTemplateHexBuildService service, int commandValue,
                                                 String commandName, Map<String, Object> values) {
        ProtocolTemplateHexBuildRequest request = new ProtocolTemplateHexBuildRequest();
        request.setBundleId(BUNDLE_ID.toString());
        request.setCommandValue((long) commandValue);
        request.setCommandName(commandName);
        request.setValues(values);
        return service.build(TENANT_ID, request);
    }

    private ProtocolTemplateHexBuildService serviceWith(ProtocolTemplateCommandDefinition cmd) {
        ProtocolTemplateBundle bundle = new ProtocolTemplateBundle();
        bundle.setId(BUNDLE_ID.toString());
        bundle.setProtocolTemplates(List.of(downlinkTemplate()));
        bundle.setProtocolCommands(List.of(cmd));
        given(bundleService.findById(eq(TENANT_ID), any(UUID.class))).willReturn(Optional.of(bundle));
        return new ProtocolTemplateHexBuildService(bundleService);
    }

    private static ProtocolTemplateDefinition downlinkTemplate() {
        ProtocolTemplateDefinition template = new ProtocolTemplateDefinition();
        template.setId(TEMPLATE_ID);
        template.setCommandByteOffset(3);
        template.setCommandMatchWidth(1);
        TcpHexChecksumDefinition none = new TcpHexChecksumDefinition();
        none.setType("NONE");
        template.setChecksum(none);
        template.setHexProtocolFields(List.of(
                field("frameStart", 0, TcpHexValueType.UINT8, 126L, false),
                field("destAddress", 1, TcpHexValueType.UINT8, null, true),
                field("frameLength", 2, TcpHexValueType.UINT8, null, true),
                field("cmdSlot", 3, TcpHexValueType.UINT8, null, true)));
        return template;
    }

    private static ProtocolTemplateCommandDefinition command(int value, String name, int frameLength) {
        ProtocolTemplateCommandDefinition cmd = new ProtocolTemplateCommandDefinition();
        cmd.setTemplateId(TEMPLATE_ID);
        cmd.setName(name);
        cmd.setCommandValue(value);
        cmd.setMatchValueType(TcpHexValueType.UINT8);
        cmd.setDirection(ProtocolTemplateCommandDirection.DOWNLINK);
        cmd.setDownlinkPayloadLengthAuto(true);
        cmd.setDownlinkPayloadLengthFieldKey("frameLength");

        TcpHexChecksumDefinition crc = new TcpHexChecksumDefinition();
        crc.setType("CRC16_IBM");
        crc.setFromByte(1);
        crc.setToExclusive(frameLength - 3);
        crc.setChecksumByteIndex(frameLength - 3);
        cmd.setInnerChecksum(crc);

        if (value == 0x29) {
            cmd.setFields(List.of(
                    field("powerLevel", 4, TcpHexValueType.UINT8, null, true),
                    field("powerValue", 5, TcpHexValueType.UINT16_LE, null, true),
                    field("frameEnd", frameLength - 1, TcpHexValueType.UINT8, 126L, false)));
        } else {
            cmd.setFields(List.of(
                    field("frameEnd", frameLength - 1, TcpHexValueType.UINT8, 126L, false)));
        }
        return cmd;
    }

    private static TcpHexFieldDefinition field(String key, int offset, TcpHexValueType type,
                                               Long fixedValue, boolean countsInPayloadLength) {
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey(key);
        f.setByteOffset(offset);
        f.setValueType(type);
        f.setFixedWireIntegralValue(fixedValue);
        f.setIncludeInDownlinkPayloadLength(countsInPayloadLength);
        return f;
    }
}
