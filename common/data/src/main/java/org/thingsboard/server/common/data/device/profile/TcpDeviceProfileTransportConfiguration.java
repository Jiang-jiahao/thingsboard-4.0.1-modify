package org.thingsboard.server.common.data.device.profile;

import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;

import java.util.Objects;

/**
 * tcp服务端传输配置
 *
 * @author jiahaozz
 * TCP 传输配置：接入模式（平台作服务端或客户端）、分帧解码、负载编码（JSON/HEX/ASCII）、链路上鉴权等。
 * <p>
 * 设备在传输配置中指定 {@link org.thingsboard.server.common.data.device.data.TcpDeviceTransportConfiguration#getServerBindPort() serverBindPort}
 * 时，该端口上的入站连接从<strong>首帧</strong>起使用本配置中的分帧与负载类型（无需依赖全局 {@code transport.tcp.server.auth_framing_mode}）。
 */
@Data
public class TcpDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    /**
     * SERVER：设备连接平台监听端口；CLIENT：平台按设备侧 host/port 主动建连。
     */
    private TcpTransportConnectMode tcpTransportConnectMode;

    /**
     * TCP 分帧：换行、长度前缀或定长。与 JSON/HEX/ASCII 负载编码独立配置。
     */
    private TcpTransportFramingMode tcpTransportFramingMode;
    /**
     * 当 {@link TcpTransportFramingMode#FIXED_LENGTH} 时必填，为每帧字节数。
     */
    private Integer tcpFixedFrameLength;

    /**
     * 链路上是否要求发送 token：{@link TcpWireAuthenticationMode#NONE} 为直连即通讯（SERVER 需配 {@code sourceHost} 绑定 IP）；
     * {@link TcpWireAuthenticationMode#TOKEN} 为启用首帧/首包 token 鉴权。
     */
    private TcpWireAuthenticationMode tcpWireAuthenticationMode;

    /**
     * CLIENT：断线或建连失败后，间隔多少秒再次尝试 outbound 建连；{@code null} 默认 30；{@code 0} 表示不重连。
     */
    private Integer tcpOutboundReconnectIntervalSec;
    /**
     * CLIENT：最大连续重连次数（每次断线或失败后计一次），{@code null} 或 {@code 0} 表示不限制。
     */
    private Integer tcpOutboundReconnectMaxAttempts;
    /**
     * CLIENT/SERVER：超过该秒数未从对端收到任何字节则关闭连接；{@code null} 或 {@code 0} 表示不启用读空闲断开。
     */
    private Integer tcpReadIdleTimeoutSec;

    /**
     * 无 {@code method} 的 JSON 上行如何入库；{@link TcpJsonWithoutMethodMode#OPAQUE_FOR_RULE_ENGINE} 时写入单键遥测供规则引擎脚本解析。
     */
    private TcpJsonWithoutMethodMode tcpJsonWithoutMethodMode;
    /**
     * {@link TcpJsonWithoutMethodMode#OPAQUE_FOR_RULE_ENGINE} 时使用的遥测键名，默认 {@code tcpOpaquePayload}。
     */
    private String tcpOpaqueRuleEngineKey;

    private TransportTcpDataTypeConfiguration transportTcpDataTypeConfiguration;


    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.TCP;
    }

    public TransportTcpDataTypeConfiguration getTransportTcpDataTypeConfiguration() {
        return Objects.requireNonNullElseGet(transportTcpDataTypeConfiguration, HexTransportTcpDataConfiguration::new);
    }

    public TcpTransportConnectMode getTcpTransportConnectMode() {
        return Objects.requireNonNullElse(tcpTransportConnectMode, TcpTransportConnectMode.SERVER);
    }

    public TcpTransportFramingMode getTcpTransportFramingMode() {
        return Objects.requireNonNullElse(tcpTransportFramingMode, TcpTransportFramingMode.LINE);
    }
    public Integer getTcpFixedFrameLength() {
        return tcpFixedFrameLength;
    }

    public TcpWireAuthenticationMode getTcpWireAuthenticationMode() {
        return Objects.requireNonNullElse(tcpWireAuthenticationMode, TcpWireAuthenticationMode.TOKEN);
    }


    public TcpJsonWithoutMethodMode getTcpJsonWithoutMethodMode() {
        return Objects.requireNonNullElse(tcpJsonWithoutMethodMode, TcpJsonWithoutMethodMode.TELEMETRY_FLAT);
    }
    public String getTcpOpaqueRuleEngineKey() {
        return Objects.requireNonNullElse(tcpOpaqueRuleEngineKey, "tcpOpaquePayload");
    }

    /**
     * {@code null} 视为 30 秒；{@code 0} 表示禁用自动重连。
     */
    public int getEffectiveTcpOutboundReconnectIntervalSec() {
        if (tcpOutboundReconnectIntervalSec == null) {
            return 30;
        }
        return tcpOutboundReconnectIntervalSec;
    }

    public boolean isTcpOutboundReconnectDisabled() {
        return tcpOutboundReconnectIntervalSec != null && tcpOutboundReconnectIntervalSec == 0;
    }

    /**
     * {@code null} 或 {@code 0}：不限制重连次数。
     */
    public int getEffectiveTcpOutboundReconnectMaxAttempts() {
        if (tcpOutboundReconnectMaxAttempts == null || tcpOutboundReconnectMaxAttempts <= 0) {
            return 0;
        }
        return tcpOutboundReconnectMaxAttempts;
    }

    /**
     * {@code null} 或 {@code 0}：不启用读空闲断开。
     */
    public int getEffectiveTcpReadIdleTimeoutSec() {
        if (tcpReadIdleTimeoutSec == null || tcpReadIdleTimeoutSec <= 0) {
            return 0;
        }
        return tcpReadIdleTimeoutSec;
    }

    @Override
    public void validate() {
        if (tcpOutboundReconnectIntervalSec != null && tcpOutboundReconnectIntervalSec < 0) {
            throw new IllegalArgumentException("tcpOutboundReconnectIntervalSec must be >= 0");
        }
        if (tcpOutboundReconnectMaxAttempts != null && tcpOutboundReconnectMaxAttempts < 0) {
            throw new IllegalArgumentException("tcpOutboundReconnectMaxAttempts must be >= 0");
        }
        if (tcpReadIdleTimeoutSec != null && tcpReadIdleTimeoutSec < 0) {
            throw new IllegalArgumentException("tcpReadIdleTimeoutSec must be >= 0");
        }
    }

}
