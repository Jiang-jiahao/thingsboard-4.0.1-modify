/**
 * Copyright © 2016-2025 The ThingsBoard Authors
 */
package org.thingsboard.server.common.data.device.profile;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TcpHexFieldDefinitionAutoTotalFrameLengthTest {

    @Test
    void validateOkWhenIntegralAndNoConflicts() {
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey("packetLen");
        f.setByteOffset(0);
        f.setValueType(TcpHexValueType.UINT32_LE);
        f.setAutoDownlinkTotalFrameLength(true);
        assertThatCode(f::validate).doesNotThrowAnyException();
    }

    @Test
    void validateFailsWhenExcludesSelfWithoutAutoTotal() {
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey("packetLen");
        f.setByteOffset(0);
        f.setValueType(TcpHexValueType.UINT32_LE);
        f.setDownlinkTotalFrameLengthExcludesLengthFieldBytes(true);
        assertThatThrownBy(f::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("downlinkTotalFrameLengthExcludesLengthFieldBytes requires autoDownlinkTotalFrameLength");
    }

    @Test
    void validateFailsWhenCombinedWithIncludeInDownlinkPayloadLength() {
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey("len");
        f.setByteOffset(0);
        f.setValueType(TcpHexValueType.UINT32_LE);
        f.setAutoDownlinkTotalFrameLength(true);
        f.setIncludeInDownlinkPayloadLength(true);
        assertThatThrownBy(f::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with includeInDownlinkPayloadLength");
    }
}
