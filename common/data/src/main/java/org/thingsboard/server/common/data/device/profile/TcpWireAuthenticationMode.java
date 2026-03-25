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
 * TCP 链路上是否要求设备发送访问令牌（与 Core 侧是否仍校验设备身份无关）。
 * <ul>
 * <li>{@link #NONE}：设备连上后按分帧规则<strong>直接发业务数据</strong>，链路上不发送 token。SERVER 需在设备上配置
 * {@link org.thingsboard.server.common.data.device.data.TcpDeviceTransportConfiguration#getSourceHost() sourceHost}
 * 与对端 IP 绑定，由传输在 Core 侧静默建会话；CLIENT 模式下平台建连后也不向设备侧发鉴权帧。</li>
 * <li>{@link #TOKEN}：<strong>启用线上鉴权</strong>时，SERVER 首帧、CLIENT 建连后首包须为含访问令牌的 JSON（与 HTTP/MQTT 一致）。</li>
 * </ul>
 */
public enum TcpWireAuthenticationMode {
    TOKEN,
    NONE
}