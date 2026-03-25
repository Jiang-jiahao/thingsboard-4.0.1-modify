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
 *   <li>可配置 {@link #serverBindPort}：平台在该监听端口上只接受映射到本设备的连接（一设备一端口）。</li>
 *   <li>无线上鉴权 {@link org.thingsboard.server.common.data.device.profile.TcpWireAuthenticationMode#NONE} 时还可配置
 *   {@link #sourceHost} 与对端 IP 匹配（可与专用端口组合使用）。</li>
 * </ul>
 */
@Data
@ToString(of = {"host", "port", "sourceHost", "serverBindPort"})
public class TcpDeviceTransportConfiguration implements DeviceTransportConfiguration {

    private String host;

    private Integer port;

    /**
     * 期望的接入源 IP（IPv4/IPv6 字符串），用于 SERVER + 无线上鉴权时的绑定；须与 socket 远端地址一致。
     */
    private String sourceHost;

    /**
     * 专用服务端监听端口（1–65535）。非空时传输进程会额外 bind 该端口；同一端口可绑定多台设备，但须共用同一设备配置文件。
     * 若多台设备且链路上鉴权为 NONE，每台须配置互异的 {@link #sourceHost}。
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