package org.thingsboard.server.common.data.device.profile;

import lombok.Data;
import org.thingsboard.server.common.data.TransportUdpDataType;

/**
 * UTF-8 文本行负载（历史枚举名 {@link org.thingsboard.server.common.data.TransportUdpDataType#JSON}）。
 */
@Data
public class JsonTransportUdpDataConfiguration implements TransportUdpDataTypeConfiguration {


    @Override
    public TransportUdpDataType getTransportUdpDataType() {
        return TransportUdpDataType.UTF8;
    }
}
