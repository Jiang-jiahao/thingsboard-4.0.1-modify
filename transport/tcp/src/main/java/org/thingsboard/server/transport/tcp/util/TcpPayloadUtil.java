/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.tcp.util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.thingsboard.server.common.data.TransportTcpDataType;
import org.thingsboard.server.common.data.device.profile.TcpTransportFramingMode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;

public final class TcpPayloadUtil {

    /**
     * {@link TransportTcpDataType#HEX}：上行把<strong>整帧原始字节</strong>格式化为小写十六进制写入该键（不再把帧当作 ASCII 十六进制文本再 parseHex）。
     * 下行：若 JSON 含此键且值为合法十六进制，则发往设备的负载为 decode 后的原始字节；否则为整段 JSON 的 UTF-8 字节。
     */
    public static final String TCP_HEX_FRAME_JSON_KEY = "hex";

    private static final byte[] CRLF = "\n".getBytes(StandardCharsets.UTF_8);
    private TcpPayloadUtil() {
    }
    /**
     * 首帧鉴权行固定为 UTF-8 JSON：{@code {"token":"..."}}，不使用 HEX 包装。
     */
    public static String encodeAuthLine(String token) {
        return "{\"token\":\"" + escapeJson(token) + "\"}";
    }
    private static String escapeJson(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
    public static String decodePayloadLine(TransportTcpDataType type, String line) {
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            if (type == TransportTcpDataType.HEX || type == TransportTcpDataType.PROTOCOL_TEMPLATE) {
                return jsonFromRawFrameAsHex(new byte[0]);
            }
            return "";
        }
        switch (type) {
            case HEX:
            case PROTOCOL_TEMPLATE:
                return jsonFromRawFrameAsHex(trimmed.getBytes(StandardCharsets.UTF_8));
            case ASCII:
            case JSON:
                return trimmed;
            default:
                return trimmed;
        }
    }
    public static String encodePayloadLine(TransportTcpDataType type, String jsonUtf8) {
        switch (type) {
            case HEX:
            case PROTOCOL_TEMPLATE:
                return HexFormat.of().formatHex(bodyBytesForDataType(TransportTcpDataType.HEX, jsonUtf8)) + "\n";
            case ASCII:
            case JSON:
                return jsonUtf8 + "\n";
            default:
                return jsonUtf8 + "\n";
        }
    }


    /**
     * 一帧原始字节 → JSON 文本（按 TransportTcpDataType 解析，再供 Gson 解析）。
     */
    public static String decodePayloadBytes(TransportTcpDataType type, byte[] frameBody) {
        if (frameBody == null || frameBody.length == 0) {
            if (type == TransportTcpDataType.HEX || type == TransportTcpDataType.PROTOCOL_TEMPLATE) {
                return jsonFromRawFrameAsHex(new byte[0]);
            }
            return "";
        }
        switch (type) {
            case HEX:
            case PROTOCOL_TEMPLATE:
                return jsonFromRawFrameAsHex(frameBody);
            case ASCII:
            case JSON:
            default:
                return new String(frameBody, StandardCharsets.UTF_8).trim();
        }
    }

    /**
     * 业务 JSON → 负载字节（HEX 时为原始字节：优先从 {@value #TCP_HEX_FRAME_JSON_KEY} 字段 parseHex，否则为整段 JSON 的 UTF-8）。
     */
    public static byte[] bodyBytesForDataType(TransportTcpDataType dataType, String jsonUtf8) {
        switch (dataType) {
            case HEX:
            case PROTOCOL_TEMPLATE:
                return rawBytesFromJsonHexDownlink(jsonUtf8);
            case ASCII:
            case JSON:
            default:
                return jsonUtf8.getBytes(StandardCharsets.UTF_8);
        }
    }
    /**
     * 鉴权 JSON 始终按 UTF-8 文本编码（与 transportTcpDataType 无关）。
     */
    public static ByteBuf encodeAuthFrame(TcpTransportFramingMode framing, int fixedFrameLength, String token) {
        byte[] inner = encodeAuthLine(token).getBytes(StandardCharsets.UTF_8);
        return wrapFraming(framing, inner, fixedFrameLength);
    }
    /**
     * 下行业务消息：先按数据类型得到负载字节，再按分帧方式封装。
     */
    public static ByteBuf encodeBusinessFrame(TransportTcpDataType dataType, TcpTransportFramingMode framing,
                                              int fixedFrameLength, String jsonUtf8) {
        byte[] inner = bodyBytesForDataType(dataType, jsonUtf8);
        return wrapFraming(framing, inner, fixedFrameLength);
    }
    public static ByteBuf wrapFraming(TcpTransportFramingMode framing, byte[] payload, int fixedFrameLength) {
        switch (framing) {
            case LINE:
                return Unpooled.wrappedBuffer(payload, CRLF);
            case LENGTH_PREFIX_4:
                ByteBuf b4 = Unpooled.buffer(4 + payload.length);
                b4.writeInt(payload.length);
                b4.writeBytes(payload);
                return b4;
            case LENGTH_PREFIX_2:
                if (payload.length > 65535) {
                    throw new IllegalArgumentException("Payload length exceeds 65535 for LENGTH_PREFIX_2");
                }
                ByteBuf b2 = Unpooled.buffer(2 + payload.length);
                b2.writeShort(payload.length);
                b2.writeBytes(payload);
                return b2;
            case FIXED_LENGTH:
                if (fixedFrameLength <= 0) {
                    throw new IllegalArgumentException("tcpFixedFrameLength required for FIXED_LENGTH framing");
                }
                if (payload.length > fixedFrameLength) {
                    throw new IllegalArgumentException("Payload length " + payload.length + " exceeds fixed frame " + fixedFrameLength);
                }
                byte[] padded = Arrays.copyOf(payload, fixedFrameLength);
                return Unpooled.wrappedBuffer(padded);
            default:
                return Unpooled.wrappedBuffer(payload, CRLF);
        }
    }

    private static String jsonFromRawFrameAsHex(byte[] frameBody) {
        String hex = HexFormat.of().formatHex(frameBody);
        return "{\"" + TCP_HEX_FRAME_JSON_KEY + "\":\"" + hex + "\"}";
    }

    private static byte[] rawBytesFromJsonHexDownlink(String jsonUtf8) {
        if (jsonUtf8 == null) {
            return new byte[0];
        }
        int keyPos = jsonUtf8.indexOf("\"" + TCP_HEX_FRAME_JSON_KEY + "\"");
        if (keyPos < 0) {
            return jsonUtf8.getBytes(StandardCharsets.UTF_8);
        }
        int colon = jsonUtf8.indexOf(':', keyPos);
        if (colon < 0) {
            return jsonUtf8.getBytes(StandardCharsets.UTF_8);
        }
        int quoteStart = jsonUtf8.indexOf('"', colon + 1);
        if (quoteStart < 0) {
            return jsonUtf8.getBytes(StandardCharsets.UTF_8);
        }
        int quoteEnd = jsonUtf8.indexOf('"', quoteStart + 1);
        if (quoteEnd <= quoteStart) {
            return jsonUtf8.getBytes(StandardCharsets.UTF_8);
        }
        String hex = jsonUtf8.substring(quoteStart + 1, quoteEnd);
        String clean = hex.replaceAll("\\s+", "");
        if (clean.isEmpty()) {
            return new byte[0];
        }
        if ((clean.length() & 1) == 1) {
            return jsonUtf8.getBytes(StandardCharsets.UTF_8);
        }
        try {
            return HexFormat.of().parseHex(clean);
        } catch (IllegalArgumentException e) {
            return jsonUtf8.getBytes(StandardCharsets.UTF_8);
        }
    }
}