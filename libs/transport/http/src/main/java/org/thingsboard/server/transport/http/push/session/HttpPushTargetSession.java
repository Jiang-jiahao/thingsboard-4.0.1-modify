package org.thingsboard.server.transport.http.push.session;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;

@Data
@Builder
public class HttpPushTargetSession {

    private DeviceId deviceId;
    private String matchKey;
    private SessionInfoProto sessionInfo;
}
