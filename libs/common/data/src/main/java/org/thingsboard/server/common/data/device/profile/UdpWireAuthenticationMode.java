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
 * Udp 链路上是否要求设备发送访问令牌（与 Core 侧是否仍校验设备身份无关）。
 * <ul>
 * <li>{@link #NONE}：设备连上后按分帧规则<strong>直接发业务数据</strong>，链路上不发送 token。SERVER 需在设备上配置
 * {@link org.thingsboard.server.common.data.device.data.UdpDeviceTransportConfiguration#getSourceHost() sourceHost}
 * 与对端 IP 绑定，由传输在 Core 侧静默建会话；CLIENT 模式下平台建连后也不向设备侧发鉴权帧。</li>
 * <li>{@link #TOKEN}：<strong>启用线上鉴权</strong>时，SERVER 首帧、CLIENT 建连后首包须为含访问令牌的 JSON（与 HTTP/MQTT 一致）。</li>
 * <li>{@link #DEFERRED_PAYLOAD_TOKEN}：<strong>延迟注册</strong>。不在链路上单独发鉴权 JSON；在 Core 会话注册前，对每一帧业务负载解析后从配置的 JSON 字段取出
 * 字符串，与设备 {@code ACCESS_TOKEN} 的 {@code credentialsId} 一致时由 Core 校验并注册会话（无该字段的帧丢弃，直至出现有效令牌）。
 * 共享端口下每帧先用全局 auth 分帧解析；识别出设备后按该档案的分帧与负载类型处理。</li>
 * <li>{@link #DEFERRED_PAYLOAD_DEVICE_ID}：<strong>延迟注册（协议设备 ID）</strong>。负载中某 JSON 字段为<strong>协议侧设备标识</strong>（非 TB 访问令牌）；
 * 按设备传输配置中的 {@link org.thingsboard.server.common.data.device.data.UdpDeviceTransportConfiguration#getUdpWireAuthPayloadDeviceId() UdpWireAuthPayloadDeviceId}
 * 在<strong>租户内</strong>查找唯一设备，再使用其 {@code ACCESS_TOKEN} 向 Core 注册会话。该协议设备 ID 须在租户内唯一。</li>
 * </ul>
 */
public enum UdpWireAuthenticationMode {
    TOKEN,
    NONE,
    DEFERRED_PAYLOAD_TOKEN,
    DEFERRED_PAYLOAD_DEVICE_ID
}