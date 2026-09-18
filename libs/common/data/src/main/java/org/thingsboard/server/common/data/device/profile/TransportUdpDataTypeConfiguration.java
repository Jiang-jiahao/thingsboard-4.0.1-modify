package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.thingsboard.server.common.data.TransportUdpDataType;

import java.io.Serializable;

/**
 * Udp传输数据类型配置
 *
 * @author jiahaozz
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "transportUdpDataType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = JsonTransportUdpDataConfiguration.class, name = "JSON"),
        @JsonSubTypes.Type(value = AsciiTransportUdpDataConfiguration.class, name = "ASCII"),
        @JsonSubTypes.Type(value = HexTransportUdpDataConfiguration.class, name = "HEX"),
        @JsonSubTypes.Type(value = ProtocolTemplateTransportUdpDataConfiguration.class, name = "PROTOCOL_TEMPLATE"),
        @JsonSubTypes.Type(value = ProtocolTemplateTransportUdpDataConfiguration.class, name = "MONITORING_PROTOCOL")})
public interface TransportUdpDataTypeConfiguration extends Serializable {

    @JsonIgnore
    TransportUdpDataType getTransportUdpDataType();

}
