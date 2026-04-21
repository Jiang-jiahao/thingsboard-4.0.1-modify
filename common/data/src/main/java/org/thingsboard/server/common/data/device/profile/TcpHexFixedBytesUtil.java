/**
 * Copyright © 2016-2025 The ThingsBoard Authors
 */
package org.thingsboard.server.common.data.device.profile;

import java.util.HexFormat;

/**
 * BYTES_AS_HEX 字段：十六进制串允许奇数位（前补 {@code 0}），若解码后短于目标字节数则左侧补零字节（大端对齐）。
 */
public final class TcpHexFixedBytesUtil {

    private TcpHexFixedBytesUtil() {
    }

    /**
     * @param hex        可含空白；奇数位时在最前补半个十六进制位 {@code 0}
     * @param byteLength 目标字节数
     * @return 恰好 {@code byteLength} 字节
     */
    public static byte[] parseHexToByteLength(String hex, int byteLength) {
        if (byteLength <= 0) {
            throw new IllegalArgumentException("byteLength must be positive");
        }
        if (hex == null) {
            return new byte[byteLength];
        }
        String clean = hex.replaceAll("\\s+", "");
        if (clean.isEmpty()) {
            return new byte[byteLength];
        }
        if ((clean.length() & 1) == 1) {
            clean = "0" + clean;
        }
        byte[] parsed;
        try {
            parsed = HexFormat.of().parseHex(clean);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid hex: " + e.getMessage());
        }
        if (parsed.length > byteLength) {
            throw new IllegalArgumentException(
                    "hex decodes to " + parsed.length + " bytes, exceeds fixed length " + byteLength);
        }
        if (parsed.length == byteLength) {
            return parsed;
        }
        byte[] out = new byte[byteLength];
        System.arraycopy(parsed, 0, out, byteLength - parsed.length, parsed.length);
        return out;
    }
}
