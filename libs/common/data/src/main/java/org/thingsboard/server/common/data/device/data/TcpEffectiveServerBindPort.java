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

import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpTransportConnectMode;

/**
 * 解析 TCP SERVER 入站「专用监听端口」：仅使用设备档案 {@link TcpDeviceProfileTransportConfiguration#getTcpProfileServerBindPort()}；
 * CLIENT 模式或非 TCP 档案时返回 {@code null}。设备传输 {@link TcpDeviceTransportConfiguration#getServerBindPort()} 不再参与解析。
 */
public final class TcpEffectiveServerBindPort {

    private TcpEffectiveServerBindPort() {
    }

    public static Integer resolve(DeviceProfile profile, TcpDeviceTransportConfiguration deviceTcp) {
        if (profile == null || profile.getProfileData() == null) {
            return null;
        }
        var tcx = profile.getProfileData().getTransportConfiguration();
        if (!(tcx instanceof TcpDeviceProfileTransportConfiguration ptc)) {
            return deviceTcp != null ? deviceTcp.getServerBindPort() : null;
        }
        if (ptc.getTcpTransportConnectMode() != TcpTransportConnectMode.SERVER) {
            return null;
        }
        return ptc.getTcpProfileServerBindPort();
    }

    /**
     * 设备档案 SERVER 模式且已配置 {@code tcpProfileServerBindPort} 时返回监听端口，否则 {@code null}。
     */
    public static Integer resolveProfileServerListenPort(DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null) {
            return null;
        }
        var tcx = profile.getProfileData().getTransportConfiguration();
        if (!(tcx instanceof TcpDeviceProfileTransportConfiguration ptc)) {
            return null;
        }
        if (ptc.getTcpTransportConnectMode() != TcpTransportConnectMode.SERVER) {
            return null;
        }
        Integer port = ptc.getTcpProfileServerBindPort();
        if (port == null || port < 1 || port > 65535) {
            return null;
        }
        return port;
    }
}
