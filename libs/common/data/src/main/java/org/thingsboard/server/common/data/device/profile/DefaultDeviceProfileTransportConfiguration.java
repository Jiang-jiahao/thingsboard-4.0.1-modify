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
package org.thingsboard.server.common.data.device.profile;

import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;

@Data
public class DefaultDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    /**
     * UI 工作模式标记：PASSIVE=被动上报，PULL=主动拉取（仅前端写入，便于保存后正确回显）。
     */
    private String httpTransportMode;

    /**
     * HTTP Push（设备主动上报）数据路由；未配置时为 null，走标准遥测解析。
     */
    private HttpPullDeviceRoutingConfiguration routing;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.DEFAULT;
    }

    @Override
    public void validate() {
        if (routing != null) {
            routing.validate();
        }
    }

}
