/**
 * Copyright © 2016-2025 The ThingsBoard Authors
 */
package org.thingsboard.server.common.data.device.profile;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TcpHexFixedBytesUtilTest {

    @Test
    void oddNibblePrependsZero() {
        assertThat(TcpHexFixedBytesUtil.parseHexToByteLength("1", 1)).isEqualTo(new byte[] {0x01});
    }

    @Test
    void padsLeadingZeroBytesToLength() {
        assertThat(TcpHexFixedBytesUtil.parseHexToByteLength("1", 4)).isEqualTo(new byte[] {0, 0, 0, 1});
    }

    @Test
    void rejectsTooLong() {
        assertThatThrownBy(() -> TcpHexFixedBytesUtil.parseHexToByteLength("0102030405", 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds");
    }

    @Test
    void utf8ZeroPadsTrailing() {
        byte[] enc = "ab".getBytes(StandardCharsets.UTF_8);
        assertThat(enc.length).isEqualTo(2);
        assertThat(TcpHexFixedBytesUtil.utf8ToZeroPaddedByteArray("ab", 4)).isEqualTo(new byte[] {'a', 'b', 0, 0});
    }

    @Test
    void utf8RejectsWhenEncodedTooLong() {
        assertThatThrownBy(() -> TcpHexFixedBytesUtil.utf8ToZeroPaddedByteArray("中", 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UTF-8");
    }

    @Test
    void unescapeCrLfThenUtf8WireMatchesCrlfBytes() {
        byte[] wire = TcpHexFixedBytesUtil.utf8FixedWireAfterUnescape("\\r\\n", 2);
        assertThat(wire).isEqualTo(new byte[] {0x0d, 0x0a});
    }

    @Test
    void alreadyRawCrLfUnchanged() {
        assertThat(TcpHexFixedBytesUtil.unescapeCStyleForFixedUtf8("\r\n")).isEqualTo("\r\n");
    }
}
