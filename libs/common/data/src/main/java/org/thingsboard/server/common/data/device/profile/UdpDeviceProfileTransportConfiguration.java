package org.thingsboard.server.common.data.device.profile;

import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;

import java.util.Objects;

/**
 * UDP 传输配置：设备向平台监听端口发送<strong>数据报</strong>（无 TCP 式 CLIENT/SERVER 建连，也无半包/粘包分帧）。
 * <p>
 * 须在档案填写 {@code udpProfileServerBindPort}；每个 UDP 报文即一条业务负载。
 * 设备传输不得再填写 {@link org.thingsboard.server.common.data.device.data.UdpDeviceTransportConfiguration#getServerBindPort() serverBindPort}。
 */
@Data
public class UdpDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    /** @deprecated 历史 JSON；UDP 仅平台监听，运行时固定为 {@link UdpTransportConnectMode#SERVER} */
    private UdpTransportConnectMode udpTransportConnectMode;

    /** @deprecated 历史 JSON；UDP 以数据报为界，运行时固定为 {@link UdpTransportFramingMode#NONE} */
    private UdpTransportFramingMode udpTransportFramingMode;
    /** @deprecated 历史 JSON，已不再使用 */
    private Integer udpFixedFrameLength;

    /**
     * 链路上是否要求发送 token：{@link UdpWireAuthenticationMode#NONE} 为直连即通讯（SERVER 需配 {@code sourceHost} 绑定 IP）；
     * {@link UdpWireAuthenticationMode#TOKEN} 为启用首帧/首包 token 鉴权；
     * {@link UdpWireAuthenticationMode#DEFERRED_PAYLOAD_TOKEN} / {@link UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID}
     * 为从业务负载（任意一帧，见枚举说明）解析身份字段后再注册会话。
     */
    private UdpWireAuthenticationMode udpWireAuthenticationMode;

    /**
     * 平台 UDP 监听端口（1–65535）：同档案下多设备共用；设备传输勿再填写 {@code serverBindPort}。
     */
    private Integer udpProfileServerBindPort;

    /**
     * 当 {@link UdpWireAuthenticationMode#DEFERRED_PAYLOAD_TOKEN} 时：解析得到的 JSON 中存放 <strong>ACCESS_TOKEN</strong>（{@code credentialsId}）的字段名。<br>
     * 当 {@link UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时：解析得到的 JSON 中存放<strong>协议设备 ID</strong>的字段名（与设备传输配置
     * {@link org.thingsboard.server.common.data.device.data.UdpDeviceTransportConfiguration#getUdpWireAuthPayloadDeviceId() udpWireAuthPayloadDeviceId} 比对，并结合入站监听端口定位设备）。<br>
     * 校验成功后该字段会从本帧重放及后续上行的副本中移除。
     */
    private String udpDeferredWireAuthTokenJsonKey;

    /** @deprecated 历史 CLIENT 模式字段，已不再使用 */
    private Integer udpOutboundReconnectIntervalSec;
    /** @deprecated 历史 CLIENT 模式字段，已不再使用 */
    private Integer udpOutboundReconnectMaxAttempts;
    /**
     * 超过该秒数未从对端收到任何 UDP 报文则关闭会话；{@code null} 或 {@code 0} 表示不启用。
     */
    private Integer udpReadIdleTimeoutSec;

    /**
     * 历史字段；当前 Udp 在 UTF-8/ASCII 下无 {@code method} 时统一写入 {@link #udpOpaqueRuleEngineKey} 单键。反序列化兼容保留。
     */
    private UdpJsonWithoutMethodMode udpJsonWithoutMethodMode;
    /**
     * 无 {@code method} 的 UTF-8/ASCII 上行写入的<strong>单一遥测键名</strong>（默认 {@code udpOpaquePayload}，与历史字段名一致）。
     */
    private String udpOpaqueRuleEngineKey;

    private TransportUdpDataTypeConfiguration transportUdpDataTypeConfiguration;


    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.UDP;
    }

    public TransportUdpDataTypeConfiguration getTransportUdpDataTypeConfiguration() {
        return Objects.requireNonNullElseGet(transportUdpDataTypeConfiguration, HexTransportUdpDataConfiguration::new);
    }

    /** UDP 无客户端/服务端建连模式，恒为平台监听入站。 */
    public UdpTransportConnectMode getUdpTransportConnectMode() {
        return UdpTransportConnectMode.SERVER;
    }

    /** 每个 UDP 数据报即一帧，不做流式分帧。 */
    public UdpTransportFramingMode getUdpTransportFramingMode() {
        return UdpTransportFramingMode.NONE;
    }
    public Integer getUdpFixedFrameLength() {
        return udpFixedFrameLength;
    }

    public UdpWireAuthenticationMode getUdpWireAuthenticationMode() {
        return Objects.requireNonNullElse(udpWireAuthenticationMode, UdpWireAuthenticationMode.TOKEN);
    }


    public UdpJsonWithoutMethodMode getUdpJsonWithoutMethodMode() {
        return Objects.requireNonNullElse(udpJsonWithoutMethodMode, UdpJsonWithoutMethodMode.TELEMETRY_FLAT);
    }
    public String getUdpOpaqueRuleEngineKey() {
        return Objects.requireNonNullElse(udpOpaqueRuleEngineKey, "udpOpaquePayload");
    }

    /**
     * {@code null} 视为 30 秒；{@code 0} 表示禁用自动重连。
     */
    public int getEffectiveUdpOutboundReconnectIntervalSec() {
        if (udpOutboundReconnectIntervalSec == null) {
            return 30;
        }
        return udpOutboundReconnectIntervalSec;
    }

    public boolean isUdpOutboundReconnectDisabled() {
        return udpOutboundReconnectIntervalSec != null && udpOutboundReconnectIntervalSec == 0;
    }

    /**
     * {@code null} 或 {@code 0}：不限制重连次数。
     */
    public int getEffectiveUdpOutboundReconnectMaxAttempts() {
        if (udpOutboundReconnectMaxAttempts == null || udpOutboundReconnectMaxAttempts <= 0) {
            return 0;
        }
        return udpOutboundReconnectMaxAttempts;
    }

    /**
     * {@code null} 或 {@code 0}：不启用读空闲断开。
     */
    public int getEffectiveUdpReadIdleTimeoutSec() {
        if (udpReadIdleTimeoutSec == null || udpReadIdleTimeoutSec <= 0) {
            return 0;
        }
        return udpReadIdleTimeoutSec;
    }

    @Override
    public void validate() {
        if (getTransportUdpDataTypeConfiguration() instanceof HexTransportUdpDataConfiguration hexCfg) {
            hexCfg.validateHexProtocolFields();
        }
        if (getTransportUdpDataTypeConfiguration() instanceof ProtocolTemplateTransportUdpDataConfiguration ptCfg) {
            ptCfg.validateProtocolTemplate();
        }
        if (udpOutboundReconnectIntervalSec != null && udpOutboundReconnectIntervalSec < 0) {
            throw new IllegalArgumentException("udpOutboundReconnectIntervalSec must be >= 0");
        }
        if (udpOutboundReconnectMaxAttempts != null && udpOutboundReconnectMaxAttempts < 0) {
            throw new IllegalArgumentException("udpOutboundReconnectMaxAttempts must be >= 0");
        }
        if (udpReadIdleTimeoutSec != null && udpReadIdleTimeoutSec < 0) {
            throw new IllegalArgumentException("udpReadIdleTimeoutSec must be >= 0");
        }
        if (getUdpWireAuthenticationMode() == UdpWireAuthenticationMode.DEFERRED_PAYLOAD_TOKEN
                || getUdpWireAuthenticationMode() == UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            if (StringUtils.isBlank(udpDeferredWireAuthTokenJsonKey)) {
                throw new IllegalArgumentException(
                        "udpDeferredWireAuthTokenJsonKey is required when udpWireAuthenticationMode is DEFERRED_PAYLOAD_TOKEN or DEFERRED_PAYLOAD_DEVICE_ID");
            }
        }
        if (udpTransportConnectMode == UdpTransportConnectMode.CLIENT) {
            throw new IllegalArgumentException("UDP transport does not support CLIENT connect mode; devices send datagrams to the platform listen port.");
        }
        if (udpTransportFramingMode != null && udpTransportFramingMode != UdpTransportFramingMode.NONE) {
            throw new IllegalArgumentException("UDP transport does not support stream framing (LINE/LENGTH_PREFIX/FIXED_LENGTH); each datagram is one payload.");
        }
        if (udpProfileServerBindPort == null) {
            throw new IllegalArgumentException("udpProfileServerBindPort is required on the UDP device profile.");
        }
        if (udpProfileServerBindPort < 1 || udpProfileServerBindPort > 65535) {
            throw new IllegalArgumentException("udpProfileServerBindPort must be between 1 and 65535");
        }
    }

}
