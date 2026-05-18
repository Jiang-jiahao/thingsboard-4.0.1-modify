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
/**
 * TCP 设备配置中的历史枚举：UTF-8/ASCII 无 {@code method} 上行已统一为「单一遥测键」（见 {@link TcpDeviceProfileTransportConfiguration#getTcpOpaqueRuleEngineKey()}）。
 * 本枚举仍用于 JSON 反序列化兼容。
 */
public enum TcpJsonWithoutMethodMode {
    TELEMETRY_FLAT,
    OPAQUE_FOR_RULE_ENGINE
}