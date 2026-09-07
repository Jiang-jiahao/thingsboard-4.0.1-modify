/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
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
