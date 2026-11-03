package com.jnks.iot.server.common.data.device.profile;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TcpHexProtocolParserCrc16IbmTest {

    private static TcpHexChecksumDefinition ibmChecksum() {
        TcpHexChecksumDefinition cs = new TcpHexChecksumDefinition();
        cs.setType("CRC16_IBM");
        cs.setFromByte(1);
        cs.setToExclusive(-3);
        cs.setChecksumByteIndex(-3);
        return cs;
    }

    /** 样例取自《主从模块通讯协议》V1.1.5 的各命令报文，校验位置先置 0 再写入。 */
    @Test
    void applyChecksumMatchesProtocolSamples() {
        assertChecksum("7e08032100007e", "7e080321412a7e");
        assertChecksum("7e20032500007e", "7e200325c0e17e");
        assertChecksum("7e00062703041000007e", "7e00062703041071b87e");
        assertChecksum("7e0204280300007e", "7e020428031e787e");
    }

    @Test
    void parsesFrameWhenChecksumValid() {
        TcpHexCommandProfile profile = commandProfile();
        assertThat(TcpHexProtocolParser.tryParseUplinkPayloadFromHex(
                JsonParser.parseString("{\"hex\":\"7e080321412a7e\"}"),
                List.of(profile), null, null, ibmChecksum(), UUID.randomUUID()))
                .isPresent()
                .get()
                .extracting(parsed -> parsed.getPayload().get("moduleAddress").getAsInt())
                .isEqualTo(8);
    }

    @Test
    void rejectsFrameWhenChecksumInvalid() {
        TcpHexCommandProfile profile = commandProfile();
        assertThat(TcpHexProtocolParser.tryParseUplinkPayloadFromHex(
                JsonParser.parseString("{\"hex\":\"7e080321412b7e\"}"),
                List.of(profile), null, null, ibmChecksum(), UUID.randomUUID()))
                .isEmpty();
    }

    private static void assertChecksum(String frameHex, String expectedHex) {
        byte[] frame = HexFormat.of().parseHex(frameHex);
        TcpHexProtocolParser.applyChecksumToFrame(ibmChecksum(), frame);
        assertThat(HexFormat.of().formatHex(frame)).isEqualTo(expectedHex);
    }

    private static TcpHexCommandProfile commandProfile() {
        TcpHexFieldDefinition address = new TcpHexFieldDefinition();
        address.setKey("moduleAddress");
        address.setByteOffset(1);
        address.setValueType(TcpHexValueType.UINT8);

        TcpHexCommandProfile profile = new TcpHexCommandProfile();
        profile.setName("0x21 设备状态查询");
        profile.setMatchByteOffset(3);
        profile.setMatchValueType(TcpHexValueType.UINT8);
        profile.setMatchValue(0x21);
        profile.setCommandMatchWidth(1);
        profile.setFields(List.of(address));
        profile.setChecksum(ibmChecksum());
        return profile;
    }
}
