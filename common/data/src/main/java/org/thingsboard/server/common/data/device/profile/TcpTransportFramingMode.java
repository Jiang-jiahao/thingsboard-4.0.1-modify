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
 * TCP 分帧方式（与负载 JSON/HEX/ASCII 编码正交：先按本枚举切出「一帧字节」，再按 TransportTcpDataType 解析帧内负载）。
 */
public enum TcpTransportFramingMode {
    /**
     * 换行符（\n 或 \r\n）分帧，适合文本协议。
     */
    LINE,
    /**
     * 帧头 4 字节无符号大端整数表示后续负载长度，不含头 4 字节。
     */
    LENGTH_PREFIX_4,
    /**
     * 帧头 2 字节无符号大端整数表示后续负载长度。
     */
    LENGTH_PREFIX_2,
    /**
     * 每帧固定字节数（需在配置中指定 {@link TcpDeviceProfileTransportConfiguration#getTcpFixedFrameLength()}）。
     */
    FIXED_LENGTH
}