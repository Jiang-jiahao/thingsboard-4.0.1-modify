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
 * 下行地址（SERVER 模式，可选）：填了 {@link #udpDownlinkHost} + {@link #udpDownlinkPort} 就发到那里，
 * 没填则回发设备最近一次上报的源地址（经透明绑定网关接入时那就是设备的真实 IP+端口）。
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
     * 可选：SERVER 模式平台<strong>下行</strong>的目标地址（IPv4/IPv6），与 {@link #udpDownlinkPort} 成对出现。
     * 填了之后 RPC / 共享属性等下发就发到这里，不再回发设备上报的源地址
     * （设备从临时/NAT 端口上报、但固定监听某个端口收指令时，上报地址的端口对不上）。
     * 留空则沿用"回发设备最近一次上报的源地址"。
     */
    private String udpDownlinkHost;

    /**
     * 可选：与 {@link #udpDownlinkHost} 成对的下行端口（1–65535）。
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