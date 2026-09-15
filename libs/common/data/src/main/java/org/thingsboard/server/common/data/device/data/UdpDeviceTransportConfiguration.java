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
 *   <li>无线上鉴权 {@link org.thingsboard.server.common.data.device.profile.UdpWireAuthenticationMode#NONE} 时还可配置
 *   {@link #sourceHost} 与对端 IP 匹配（前置 LB 时看到的是 LB 的源 IP，该模式需透明代理）。</li>
 *   <li>链路上鉴权 {@link org.thingsboard.server.common.data.device.profile.UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时须配置
 *   {@link #udpWireAuthPayloadDeviceId}：与负载 JSON 中档案所配字段值一致，且在同一租户内多设备时值须互异。</li>
 * </ul>
 */
@Data
@ToString(of = {"host", "port", "sourceHost", "udpWireAuthPayloadDeviceId"})
public class UdpDeviceTransportConfiguration implements DeviceTransportConfiguration {

    private String host;

    private Integer port;

    /**
     * 期望的接入源 IP（IPv4/IPv6 字符串），用于 SERVER + 无线上鉴权时的绑定；须与 socket 远端地址一致。
     */
    private String sourceHost;

    /**
     * 当设备档案为 {@link org.thingsboard.server.common.data.device.profile.UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时：
     * 与上行解析 JSON 中「协议设备 ID」字段值一致，用于区分 TB 设备（同一租户内须唯一）。
     */
    private String udpWireAuthPayloadDeviceId;

    public UdpDeviceTransportConfiguration() {
        this.host = "127.0.0.1";
        this.port = 5025;
    }
    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.UDP;
    }
    @Override
    public void validate() {
        if (!isValid()) {
            throw new IllegalArgumentException("Udp transport: set host+port for CLIENT, or sourceHost for SERVER");
        }
    }
    @JsonIgnore
    private boolean isValid() {
        if (StringUtils.isNotBlank(sourceHost)) {
            return true;
        }
        return StringUtils.isNotBlank(host) && port != null && port > 0 && port <= 65535;
    }
}