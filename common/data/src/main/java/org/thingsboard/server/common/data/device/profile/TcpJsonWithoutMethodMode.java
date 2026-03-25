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
 * TCP 上行 JSON 无 {@code method} 字段时的处理方式。
 * <ul>
 *   <li>{@link #TELEMETRY_FLAT}：根对象各键作为遥测时序写入（历史行为）。</li>
 *   <li>{@link #OPAQUE_FOR_RULE_ENGINE}：整帧 JSON 序列化后写入单个遥测键，由规则链脚本解析并分流到遥测/属性等。</li>
 * </ul>
 */
public enum TcpJsonWithoutMethodMode {
    TELEMETRY_FLAT,
    OPAQUE_FOR_RULE_ENGINE
}