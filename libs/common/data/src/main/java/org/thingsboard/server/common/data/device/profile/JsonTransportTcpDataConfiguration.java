package org.thingsboard.server.common.data.device.profile;

import lombok.Data;
import org.thingsboard.server.common.data.TransportTcpDataType;

/**
 * UTF-8 文本行负载（历史枚举名 {@link org.thingsboard.server.common.data.TransportTcpDataType#JSON}）。
 */
@Data
public class JsonTransportTcpDataConfiguration implements TransportTcpDataTypeConfiguration {


    @Override
    public TransportTcpDataType getTransportTcpDataType() {
        return TransportTcpDataType.UTF8;
    }
}
