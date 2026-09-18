package org.thingsboard.server.common.data.device.profile;

import lombok.Data;
import org.thingsboard.server.common.data.TransportTcpDataType;

/**
 * ascii的tcp传输数据
 *
 * @author jiahaozz
 */
@Data
public class AsciiTransportTcpDataConfiguration implements TransportTcpDataTypeConfiguration {


    @Override
    public TransportTcpDataType getTransportTcpDataType() {
        return TransportTcpDataType.ASCII;
    }
}
