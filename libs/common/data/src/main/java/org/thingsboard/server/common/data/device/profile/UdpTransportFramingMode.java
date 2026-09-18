package org.thingsboard.server.common.data.device.profile;
/**
 * Udp 分帧方式（与负载 JSON/HEX/ASCII 编码正交：先按本枚举切出「一帧字节」，再按 TransportUdpDataType 解析帧内负载）。
 */
public enum UdpTransportFramingMode {
    /**
     * 不做分帧处理：socket 读取到的字节块直接交给负载解码。
     */
    NONE,
    /**
     * 换行符（\n 或 \r\n）分帧，适合文本协议。
     */
    LINE,
    /**
     * 帧头 4 字节无符号大端整数表示后续负载长度，不含头 4 字节。
     */
    LENGTH_PREFIX_4,
    /**
     * 帧头 2 字节无符号大端整数表示后续负载长度。
     */
    LENGTH_PREFIX_2,
    /**
     * 每帧固定字节数（需在配置中指定 {@link UdpDeviceProfileTransportConfiguration#getUdpFixedFrameLength()}）。
     */
    FIXED_LENGTH
}