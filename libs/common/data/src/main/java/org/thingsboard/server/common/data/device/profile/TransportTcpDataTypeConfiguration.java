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
