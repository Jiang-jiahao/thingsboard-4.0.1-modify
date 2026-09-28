package com.jnks.iot.server.transport.udp.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * PROXY protocol v2 解析（只解析头，不做分帧）。
 * <p>
 * 前面挂了 L4 代理（nginx stream 的 {@code proxy_protocol on}）时，数据报前面会多一段 PROXY v2 头，
 * 真实源地址在其中；传输层用它替代 {@code packet.sender()}，这样：
 * <ul>
 *   <li>NONE 鉴权的「期望源 IP（sourceHost）」以及按来源区分设备仍然成立；</li>
 *   <li>下行回包发给真实设备地址（而不是代理），设备看到的源端口也还是档案声明的监听端口。</li>
 * </ul>
 * 没有协议头的数据报原样处理（自研设备直接连平台时就是这样）。
 *
 * @see <a href="https://www.haproxy.org/download/2.8/doc/proxy-protocol.txt">PROXY protocol 规范</a>
 */
public final class UdpProxyProtocol {

    /** v2 固定签名 12 字节：\r\n\r\n\0\r\nQUIT\n */
    private static final byte[] V2_SIGNATURE = {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};
    /** v1 文本头前缀："PROXY " */
    private static final byte[] V1_PREFIX = {'P', 'R', 'O', 'X', 'Y', ' '};

    private UdpProxyProtocol() {
    }

    /**
     * 解析结果：真实源地址、目的地址、以及负载起始偏移（去掉头之后的业务数据从这里开始）。
     */
    public record Header(InetSocketAddress source, InetSocketAddress destination, int payloadOffset) {
    }

    /** 是否像 PROXY protocol 头（v2 二进制 或 v1 文本）。 */
    public static boolean looksLikeHeader(byte[] data) {
        return data != null && (startsWith(data, V2_SIGNATURE) || startsWith(data, V1_PREFIX));
    }

    /**
     * 解析 PROXY 头；不是协议头、或版本/地址族不支持时返回 {@code null}（调用方按普通报文处理）。
     */
    public static Header parse(byte[] data) {
        if (data == null) {
            return null;
        }
        if (startsWith(data, V1_PREFIX)) {
            return parseV1(data);
        }
        if (!startsWith(data, V2_SIGNATURE)) {
            return null;
        }
        return parseV2(data);
    }

    /**
     * v1 是文本行：{@code PROXY TCP4|TCP6|UDP4|UDP6 srcIP dstIP srcPort dstPort\r\n}；
     * {@code PROXY UNKNOWN} 表示无地址信息。nginx 对 UDP 转发发的就是这种。
     */
    private static Header parseV1(byte[] data) {
        int crlf = -1;
        int limit = Math.min(data.length, 108);      // 规范里 v1 行最长 107 字节
        for (int i = 0; i < limit - 1; i++) {
            if (data[i] == '\r' && data[i + 1] == '\n') {
                crlf = i;
                break;
            }
        }
        if (crlf < 0) {
            return null;
        }
        String line = new String(data, 0, crlf, java.nio.charset.StandardCharsets.US_ASCII);
        String[] parts = line.trim().split("\\s+");
        if (parts.length < 6 || !"PROXY".equals(parts[0])) {
            return null;
        }
        String proto = parts[1];
        if (!proto.startsWith("TCP") && !proto.startsWith("UDP")) {
            return null;                              // PROXY UNKNOWN 等
        }
        try {
            InetAddress src = InetAddress.getByName(parts[2]);
            InetAddress dst = InetAddress.getByName(parts[3]);
            int srcPort = Integer.parseInt(parts[4]);
            int dstPort = Integer.parseInt(parts[5]);
            return new Header(new InetSocketAddress(src, srcPort), new InetSocketAddress(dst, dstPort), crlf + 2);
        } catch (UnknownHostException | NumberFormatException e) {
            return null;
        }
    }

    private static Header parseV2(byte[] data) {
        if (data.length < 16) {
            return null;
        }
        int versionCommand = data[12] & 0xFF;
        if ((versionCommand >> 4) != 0x2) {
            return null;   // 只支持 v2
        }
        int command = versionCommand & 0x0F;
        if (command != 0x1) {
            return null;   // 0x0 = LOCAL（健康检查，无地址信息）
        }
        int familyProtocol = data[13] & 0xFF;
        int addressLen = ((data[14] & 0xFF) << 8) | (data[15] & 0xFF);
        int payloadOffset = 16 + addressLen;
        if (data.length < payloadOffset) {
            return null;
        }
        int family = familyProtocol >> 4;
        try {
            if (family == 0x1) {                 // AF_INET
                if (addressLen < 12) {
                    return null;
                }
                InetAddress src = InetAddress.getByAddress(slice(data, 16, 4));
                InetAddress dst = InetAddress.getByAddress(slice(data, 20, 4));
                int srcPort = ((data[24] & 0xFF) << 8) | (data[25] & 0xFF);
                int dstPort = ((data[26] & 0xFF) << 8) | (data[27] & 0xFF);
                return new Header(new InetSocketAddress(src, srcPort), new InetSocketAddress(dst, dstPort), payloadOffset);
            }
            if (family == 0x2) {                 // AF_INET6
                if (addressLen < 36) {
                    return null;
                }
                InetAddress src = InetAddress.getByAddress(slice(data, 16, 16));
                InetAddress dst = InetAddress.getByAddress(slice(data, 32, 16));
                int srcPort = ((data[48] & 0xFF) << 8) | (data[49] & 0xFF);
                int dstPort = ((data[50] & 0xFF) << 8) | (data[51] & 0xFF);
                return new Header(new InetSocketAddress(src, srcPort), new InetSocketAddress(dst, dstPort), payloadOffset);
            }
        } catch (UnknownHostException e) {
            return null;
        }
        return null;   // AF_UNIX 等不支持
    }

    private static boolean startsWith(byte[] data, byte[] prefix) {
        if (data.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] slice(byte[] data, int offset, int len) {
        byte[] out = new byte[len];
        System.arraycopy(data, offset, out, 0, len);
        return out;
    }
}
