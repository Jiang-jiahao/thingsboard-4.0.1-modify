package org.thingsboard.server.common.data.device.profile;

import lombok.Data;
import org.thingsboard.server.common.data.TransportUdpDataType;

/**
 * ascii的Udp传输数据
 *
 * @author jiahaozz
 */
@Data
public class AsciiTransportUdpDataConfiguration implements TransportUdpDataTypeConfiguration {


    @Override
    public TransportUdpDataType getTransportUdpDataType() {
        return TransportUdpDataType.ASCII;
    }
}
