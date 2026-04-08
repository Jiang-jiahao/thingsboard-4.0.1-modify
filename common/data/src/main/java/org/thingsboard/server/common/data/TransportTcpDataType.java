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
package org.thingsboard.server.common.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum TransportTcpDataType {
    JSON,
    /**
     * 一帧负载为<strong>原始字节</strong>；传输层将其直接格式化为十六进制并包成 JSON（键名 {@code hex}），不再把帧当作 ASCII 十六进制文本去还原内层 UTF-8。
     */
    HEX,
    /**
     * 与 {@link #HEX} 链路上行为相同；设备配置使用 {@link org.thingsboard.server.common.data.device.profile.ProtocolTemplateTransportTcpDataConfiguration}
     * 按「帧模板 + 上行/下行命令」建模后展开为 HEX 解析。
     */
    PROTOCOL_TEMPLATE,
    ASCII;

    @JsonValue
    public String toJson() {
        return name();
    }

    /**
     * 兼容历史配置中 {@code MONITORING_PROTOCOL} 字符串。
     */
    @JsonCreator
    public static TransportTcpDataType fromJson(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        if ("MONITORING_PROTOCOL".equals(value)) {
            return PROTOCOL_TEMPLATE;
        }
        return TransportTcpDataType.valueOf(value);
    }
}
