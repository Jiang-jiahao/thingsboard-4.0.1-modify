package com.jnks.iot.server.common.data.device.data;
import lombok.Data;
import lombok.ToString;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.StringUtils;
/**
 * UDP 设备传输配置。
 * <p>
 * 无线上鉴权 {@link com.jnks.iot.server.common.data.device.profile.UdpWireAuthenticationMode#NONE} 时可配置
 * {@link #sourceHost} 与对端 IP 匹配（前置 LB 时看到的是 LB 的源 IP，该模式需透明代理）。
 * <p>
 * 链路上鉴权 {@link com.jnks.iot.server.common.data.device.profile.UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时须配置
 * {@link #udpWireAuthPayloadDeviceId}：与负载 JSON 中档案所配字段值一致，且在同一租户内多设备时值须互异。
 * <p>
 * 下行地址：默认回发到设备<strong>最近一次上报</strong>的源 IP:源端口；若设备的上报端口与接收指令的端口不同
 * （设备从临时端口上报、固定端口收指令），用 {@link #udpDownlinkHost} + {@link #udpDownlinkPort} 指定固定下行地址。
 */
@Data
@ToString(of = {"sourceHost", "udpWireAuthPayloadDeviceId", "udpDownlinkHost", "udpDownlinkPort"})
public class UdpDeviceTransportConfiguration implements DeviceTransportConfiguration {

    /** @deprecated 历史 CLIENT 模式字段，已不再使用（UDP 无平台主动建连） */
    private String host;

    /** @deprecated 历史 CLIENT 模式字段，已不再使用（UDP 无平台主动建连） */
    private Integer port;

    /**
     * 期望的接入源 IP（IPv4/IPv6 字符串），用于 SERVER + 无线上鉴权时的绑定；须与 socket 远端地址一致。
     */
    private String sourceHost;

    /**
     * 当设备档案为 {@link com.jnks.iot.server.common.data.device.profile.UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时：
     * 与上行解析 JSON 中「协议设备 ID」字段值一致，用于区分 TB 设备（同一租户内须唯一）。
     */
    private String udpWireAuthPayloadDeviceId;

    /**
     * 可选：平台<strong>下行</strong>的固定目标地址（IPv4/IPv6），与 {@link #udpDownlinkPort} 成对出现。
     * 设了之后，RPC / 共享属性等下发不再回发到设备最近上报的源地址，而是发到这里
     * （典型场景：设备从临时/NAT 端口上报，但固定监听某个端口收指令）。
     * 留空则沿用"回发设备最近上报的源地址"。
     */
    private String udpDownlinkHost;

    /**
     * 可选：与 {@link #udpDownlinkHost} 成对的固定下行端口（1–65535）。
     */
    private Integer udpDownlinkPort;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.UDP;
    }

    @Override
    public void validate() {
        boolean hostSet = StringUtils.isNotBlank(udpDownlinkHost);
        boolean portSet = udpDownlinkPort != null;
        if (hostSet != portSet) {
            throw new IllegalArgumentException("udpDownlinkHost and udpDownlinkPort must be set together (or both left empty)");
        }
        if (portSet && (udpDownlinkPort < 1 || udpDownlinkPort > 65535)) {
            throw new IllegalArgumentException("udpDownlinkPort must be between 1 and 65535");
        }
    }
}