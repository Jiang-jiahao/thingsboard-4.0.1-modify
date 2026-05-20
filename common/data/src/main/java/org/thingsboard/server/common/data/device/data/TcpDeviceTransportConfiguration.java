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
package org.thingsboard.server.common.data.device.data;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.ToString;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
/**
 * CLIENT 模式下平台主动连接设备时使用：目标设备地址与端口。
 * <p>
 * SERVER 模式：
 * <ul>
 *   <li>优先在设备档案配置 {@code tcpProfileServerBindPort}；未配置档案端口时可在此配置 {@link #serverBindPort}（兼容旧数据）。</li>
 *   <li>无线上鉴权 {@link org.thingsboard.server.common.data.device.profile.TcpWireAuthenticationMode#NONE} 时还可配置
 *   {@link #sourceHost} 与对端 IP 匹配（可与专用端口组合使用）。</li>
 *   <li>链路上鉴权 {@link org.thingsboard.server.common.data.device.profile.TcpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时须配置
 *   {@link #tcpWireAuthPayloadDeviceId}：与负载 JSON 中档案所配字段值一致，且在同一专用监听端口下多设备时值须互异。</li>
 * </ul>
 */
@Data
@ToString(of = {"host", "port", "sourceHost", "serverBindPort", "tcpWireAuthPayloadDeviceId"})
public class TcpDeviceTransportConfiguration implements DeviceTransportConfiguration {

    private String host;

    private Integer port;

    /**
     * 期望的接入源 IP（IPv4/IPv6 字符串），用于 SERVER + 无线上鉴权时的绑定；须与 socket 远端地址一致。
     */
    private String sourceHost;

    /**
     * 当设备档案为 {@link org.thingsboard.server.common.data.device.profile.TcpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时：
     * 与上行解析 JSON 中「协议设备 ID」字段值一致，用于在共用专用监听端口下区分 TB 设备（可与其它端口下设备使用相同字符串）。
     */
    private String tcpWireAuthPayloadDeviceId;

    /**
     * 兼容旧版：设备级专用监听端口。若设备档案已配置 {@code tcpProfileServerBindPort}，此处须留空，由档案统一指定入口端口。
     */
    private Integer serverBindPort;

    public TcpDeviceTransportConfiguration() {
        this.host = "127.0.0.1";
        this.port = 5025;
    }
    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.TCP;
    }
    @Override
    public void validate() {
        if (!isValid()) {
            throw new IllegalArgumentException("TCP transport: set host+port for CLIENT, or sourceHost / serverBindPort for SERVER");
        }
    }
    @JsonIgnore
    private boolean isValid() {
        if (serverBindPort != null && (serverBindPort < 1 || serverBindPort > 65535)) {
            return false;
        }
        if (StringUtils.isNotBlank(sourceHost)) {
            return true;
        }
        if (serverBindPort != null) {
            return true;
        }
        return StringUtils.isNotBlank(host) && port != null && port > 0 && port <= 65535;
    }
}