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
package org.thingsboard.server.transport.tcp;
import lombok.Getter;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.device.profile.TcpTransportFramingMode;
/**
 * 入站连接在 pipeline 首段使用的分帧参数；专用端口上来自设备配置文件，否则来自全局 transport.tcp.server.*。
 */
@Getter
public final class TcpInboundPipelineConfig {
    private final TcpTransportFramingMode framingMode;
    private final int fixedFrameLength;
    private final DeviceProfile deviceProfile;
    public TcpInboundPipelineConfig(TcpTransportFramingMode framingMode, int fixedFrameLength, DeviceProfile deviceProfile) {
        this.framingMode = framingMode;
        this.fixedFrameLength = fixedFrameLength;
        this.deviceProfile = deviceProfile;
    }
}