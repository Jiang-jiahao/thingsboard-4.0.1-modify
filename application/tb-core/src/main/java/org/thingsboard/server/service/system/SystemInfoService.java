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
package org.thingsboard.server.service.system;

import org.thingsboard.server.common.data.FeaturesInfo;
import org.thingsboard.server.common.data.SystemInfo;

/**
 * 系统信息查询接口：集群/单机运行指标与功能开关状态。
 */
public interface SystemInfoService {
    /** 获取当前系统/集群资源占用信息。 */
    SystemInfo getSystemInfo();

    /** 获取邮件、短信、OAuth2、2FA、Slack 等功能是否已配置。 */
    FeaturesInfo getFeaturesInfo();
}
