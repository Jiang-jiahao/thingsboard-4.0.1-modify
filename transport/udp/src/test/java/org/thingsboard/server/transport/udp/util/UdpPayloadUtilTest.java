/**
 * Copyright © 2016-2025 The Thingsboard Authors
 */
package org.thingsboard.server.transport.udp.util;

import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.data.TransportUdpDataType;
import org.thingsboard.server.common.data.device.profile.UdpTransportFramingMode;

import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UdpPayloadUtilTest {

    @Test
    void isHexTemplateRpcParams_detectsHexKey() {
        assertTrue(UdpPayloadUtil.isHexTemplateRpcParams("{\"hex\":\"0102\"}"));
        assertFalse(UdpPayloadUtil.isHexTemplateRpcParams("{\"cmd\":1}"));
        assertFalse(UdpPayloadUtil.isHexTemplateRpcParams(null));
    }

    @Test
    void encodeBusinessFrame_fromRpcParams_sendsRawBytesOnly() {
        String hex = "1c00000000000000000000000a0000000c0000000001010100000000";
        String params = "{\"hex\":\"" + hex + "\"}";
        byte[] expected = HexFormat.of().parseHex(hex);
        byte[] body = UdpPayloadUtil.bodyBytesForDataType(TransportUdpDataType.PROTOCOL_TEMPLATE, params);
        assertArrayEquals(expected, body);
    }

    @Test
    void encodeBusinessFrame_lineFraming_appendsCrlf() {
        byte[] inner = UdpPayloadUtil.bodyBytesForDataType(TransportUdpDataType.RAW_BYTES, "{\"hex\":\"0102\"}");
        var buf = UdpPayloadUtil.encodeBusinessFrame(
                TransportUdpDataType.RAW_BYTES,
                UdpTransportFramingMode.LINE,
                0,
                "{\"hex\":\"0102\"}");
        byte[] framed = new byte[buf.readableBytes()];
        buf.readBytes(framed);
        buf.release();
        assertArrayEquals(new byte[] {0x01, 0x02, '\n'}, framed);
        assertArrayEquals(new byte[] {0x01, 0x02}, inner);
    }

    @Test
    void encodeBusinessFrame_noneFraming_noSuffix() {
        var buf = UdpPayloadUtil.encodeBusinessFrame(
                TransportUdpDataType.RAW_BYTES,
                UdpTransportFramingMode.NONE,
                0,
                "{\"hex\":\"0102\"}");
        byte[] framed = new byte[buf.readableBytes()];
        buf.readBytes(framed);
        buf.release();
        assertArrayEquals(new byte[] {0x01, 0x02}, framed);
    }
}
