/**
 * Copyright © 2016-2025 The ThingsBoard Authors
 */
package org.thingsboard.server.common.data.device.profile;

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
}
