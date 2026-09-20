package com.jnks.iot.server.common.data.device.profile;

import lombok.Data;
import com.jnks.iot.server.common.data.TransportTcpDataType;

/**
 * UTF-8 文本行负载（历史枚举名 {@link com.jnks.iot.server.common.data.TransportTcpDataType#JSON}）。
 */
@Data
public class JsonTransportTcpDataConfiguration implements TransportTcpDataTypeConfiguration {


    @Override
    public TransportTcpDataType getTransportTcpDataType() {
        return TransportTcpDataType.UTF8;
    }
}
