package com.jnks.iot.server.common.data.device.data;
import lombok.Data;
import lombok.ToString;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.StringUtils;
/**
 * UDP 设备传输配置。
 * <p>
 * 无线上鉴权 {@link com.jnks.iot.server.common.data.device.profile.UdpWireAuthenticationMode#NONE} 时可配置
 * {@link #sourceHost} 与对端 IP 匹配（前置网关须为**透明绑定**，即 nginx {@code proxy_bind ... transparent}，
 * 否则传输侧看到的是网关地址而不是设备地址）。
 * <p>
 * 链路上鉴权 {@link com.jnks.iot.server.common.data.device.profile.UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时须配置
 * {@link #udpWireAuthPayloadDeviceId}：与负载 JSON 中档案所配字段值一致，且在同一租户内多设备时值须互异。
 * <p>
 * 下行地址（<strong>必填</strong>）：平台下行只会发到 {@link #udpDownlinkHost} + {@link #udpDownlinkPort}
 * —— 没配地址的设备**收不到** RPC/属性下发（2026-10-12 起不再回退到"回发上报的源地址"，
 * 与 HTTP 一样：下发要显式配置）。
 */
@Data
@ToString(of = {"sourceHost", "udpWireAuthPayloadDeviceId", "udpDownlinkHost", "udpDownlinkPort"})
public class UdpDeviceTransportConfiguration implements DeviceTransportConfiguration {

    /**
     * 期望的接入源 IP（IPv4/IPv6 字符串），用于无线上鉴权时的绑定；须与 socket 远端地址一致。
     */
    private String sourceHost;

    /**
     * 当设备档案为 {@link com.jnks.iot.server.common.data.device.profile.UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时：
     * 与上行解析 JSON 中「协议设备 ID」字段值一致，用于区分 TB 设备（同一租户内须唯一）。
     */
    private String udpWireAuthPayloadDeviceId;

    /**
     * <strong>必填</strong>：平台<strong>下行</strong>的目标地址（IPv4/IPv6），与 {@link #udpDownlinkPort} 成对。
     * RPC / 属性响应等下发都发到这里（设备从临时/NAT 端口上报、但固定监听某个端口收指令时，上报地址的端口也对不上）。
     */
    private String udpDownlinkHost;

    /**
     * <strong>必填</strong>：与 {@link #udpDownlinkHost} 成对的下行端口（1–65535）。
     */
    private Integer udpDownlinkPort;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.UDP;
    }

    @Override
    public void validate() {
        if (StringUtils.isBlank(udpDownlinkHost) || udpDownlinkPort == null) {
            throw new IllegalArgumentException(
                    "udpDownlinkHost and udpDownlinkPort are required: the platform only sends UDP downlink frames to this fixed address");
        }
        if (udpDownlinkPort < 1 || udpDownlinkPort > 65535) {
            throw new IllegalArgumentException("udpDownlinkPort must be between 1 and 65535");
        }
    }
}