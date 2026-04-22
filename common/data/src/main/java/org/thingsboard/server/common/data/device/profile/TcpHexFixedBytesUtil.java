/**
 * Copyright © 2016-2025 The ThingsBoard Authors
 */
package org.thingsboard.server.common.data.device.profile;

import java.nio.charset.StandardCharsets;
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

    /**
     * 命令字等「定长线」匹配：十六进制位数必须恰好为 {@code byteLength * 2}（可含空白；可选 {@code 0x} 前缀）。
     */
    public static byte[] parseHexExactWireBytes(String hex, int byteLength) {
        if (byteLength <= 0) {
            throw new IllegalArgumentException("byteLength must be positive");
        }
        if (hex == null) {
            throw new IllegalArgumentException("hex is required");
        }
        String c = hex.replaceAll("\\s+", "");
        if (c.regionMatches(true, 0, "0x", 0, 2)) {
            c = c.substring(2);
        }
        if (c.length() != byteLength * 2) {
            throw new IllegalArgumentException(
                    "hex must have exactly " + (byteLength * 2) + " hex digits for wire width " + byteLength + " bytes");
        }
        try {
            return HexFormat.of().parseHex(c);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid hex: " + e.getMessage());
        }
    }

    /**
     * BYTES_AS_UTF8 固定字段：将字面量反斜杠转义（与常见 JSON/表单写法一致）还原为控制字符后再定长编码。
     * 支持：{@code \r}、{@code \n}、{@code \t}、{@code \\}、{@code \0}；其它 {@code \x} 仅保留反斜杠，x 留给下一轮循环处理。
     */
    public static String unescapeCStyleForFixedUtf8(String raw) {
        if (raw == null || raw.isEmpty()) {
            return raw;
        }
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '\\' && i + 1 < raw.length()) {
                char e = raw.charAt(i + 1);
                switch (e) {
                    case 'r' -> {
                        sb.append('\r');
                        i++;
                    }
                    case 'n' -> {
                        sb.append('\n');
                        i++;
                    }
                    case 't' -> {
                        sb.append('\t');
                        i++;
                    }
                    case '\\' -> {
                        sb.append('\\');
                        i++;
                    }
                    case '0' -> {
                        sb.append('\0');
                        i++;
                    }
                    default -> sb.append(c);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * 先 {@link #unescapeCStyleForFixedUtf8}，再 {@link #utf8ToZeroPaddedByteArray}（校验 / 上行比对 / 下行组帧共用）。
     */
    public static byte[] utf8FixedWireAfterUnescape(String raw, int byteLength) {
        return utf8ToZeroPaddedByteArray(unescapeCStyleForFixedUtf8(raw), byteLength);
    }

    /**
     * BYTES_AS_UTF8 固定字段：明文字符串按 UTF-8 编码后右侧零填充至 {@code byteLength}；编码超长则抛异常（与组帧、上行比对一致）。
     */
    public static byte[] utf8ToZeroPaddedByteArray(String text, int byteLength) {
        if (byteLength <= 0) {
            throw new IllegalArgumentException("byteLength must be positive");
        }
        byte[] enc = text == null ? new byte[0] : text.getBytes(StandardCharsets.UTF_8);
        if (enc.length > byteLength) {
            throw new IllegalArgumentException(
                    "UTF-8 encodes to " + enc.length + " bytes, exceeds fixed length " + byteLength);
        }
        byte[] out = new byte[byteLength];
        System.arraycopy(enc, 0, out, 0, enc.length);
        return out;
    }
}
