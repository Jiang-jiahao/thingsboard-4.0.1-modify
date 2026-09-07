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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.thingsboard.server.common.data.TransportTcpDataType;

import java.io.Serializable;

/**
 * TCP传输数据类型配置
 *
 * @author jiahaozz
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "transportTcpDataType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = JsonTransportTcpDataConfiguration.class, name = "JSON"),
        @JsonSubTypes.Type(value = AsciiTransportTcpDataConfiguration.class, name = "ASCII"),
        @JsonSubTypes.Type(value = HexTransportTcpDataConfiguration.class, name = "HEX"),
        @JsonSubTypes.Type(value = ProtocolTemplateTransportTcpDataConfiguration.class, name = "PROTOCOL_TEMPLATE"),
        @JsonSubTypes.Type(value = ProtocolTemplateTransportTcpDataConfiguration.class, name = "MONITORING_PROTOCOL")})
public interface TransportTcpDataTypeConfiguration extends Serializable {

    @JsonIgnore
    TransportTcpDataType getTransportTcpDataType();

}
