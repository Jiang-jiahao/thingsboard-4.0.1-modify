package org.thingsboard.server.common.data.device.profile;

import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;

import java.util.Objects;

/**
 * tcp服务端传输配置
 *
 * @author jiahaozz
 */
@Data
public class TcpDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    private TransportTcpDataTypeConfiguration transportTcpDataTypeConfiguration;


    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.TCP;
    }

    public TransportTcpDataTypeConfiguration getTransportTcpDataTypeConfiguration() {
        return Objects.requireNonNullElseGet(transportTcpDataTypeConfiguration, HexTransportTcpDataConfiguration::new);
    }



}
