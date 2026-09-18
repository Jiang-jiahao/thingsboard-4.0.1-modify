package org.thingsboard.server.common.data.device.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.swagger.v3.oas.annotations.media.Schema;
import org.thingsboard.server.common.data.DeviceTransportType;

import java.io.Serializable;

@Schema
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DefaultDeviceTransportConfiguration.class, name = "DEFAULT"),
        @JsonSubTypes.Type(value = MqttDeviceTransportConfiguration.class, name = "MQTT"),
        @JsonSubTypes.Type(value = CoapDeviceTransportConfiguration.class, name = "COAP"),
        @JsonSubTypes.Type(value = Lwm2mDeviceTransportConfiguration.class, name = "LWM2M"),
        @JsonSubTypes.Type(value = SnmpDeviceTransportConfiguration.class, name = "SNMP"),
        @JsonSubTypes.Type(value = TcpDeviceTransportConfiguration.class, name = "TCP"),
        @JsonSubTypes.Type(value = UdpDeviceTransportConfiguration.class, name = "UDP"),
        @JsonSubTypes.Type(value = HttpPullDeviceTransportConfiguration.class, name = "HTTP_PULL"),
        @JsonSubTypes.Type(value = MqttPullDeviceTransportConfiguration.class, name = "MQTT_PULL")})
public interface DeviceTransportConfiguration extends Serializable {
    @JsonIgnore
    DeviceTransportType getType();

    default void validate() {
    }

}
