package org.thingsboard.server.common.data.device.profile;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TcpHexFieldDefinitionNormalizeTest {

    @Test
    void validateClearsIntegralWhenBytesAsHexAndBothFixedSet() {
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey("x");
        f.setByteOffset(0);
        f.setValueType(TcpHexValueType.BYTES_AS_HEX);
        f.setByteLength(1);
        f.setFixedWireIntegralValue(16L);
        f.setFixedBytesHex("10");
        f.validate();
        assertThat(f.getFixedWireIntegralValue()).isNull();
        assertThat(f.getFixedBytesHex()).isEqualTo("10");
    }

    @Test
    void validateClearsHexWhenIntegralTypeAndBothFixedSet() {
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey("x");
        f.setByteOffset(0);
        f.setValueType(TcpHexValueType.UINT8);
        f.setFixedWireIntegralValue(16L);
        f.setFixedBytesHex("10");
        f.validate();
        assertThat(f.getFixedBytesHex()).isNull();
        assertThat(f.getFixedWireIntegralValue()).isEqualTo(16L);
    }
}
