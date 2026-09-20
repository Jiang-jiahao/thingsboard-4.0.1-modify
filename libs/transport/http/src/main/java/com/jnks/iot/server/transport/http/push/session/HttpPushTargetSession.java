package com.jnks.iot.server.transport.http.push.session;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionInfoProto;

@Data
@Builder
public class HttpPushTargetSession {

    private DeviceId deviceId;
    private String matchKey;
    private SessionInfoProto sessionInfo;
}
