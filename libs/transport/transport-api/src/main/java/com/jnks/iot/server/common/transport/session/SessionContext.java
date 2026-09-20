package com.jnks.iot.server.common.transport.session;

import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.Optional;
import java.util.UUID;

public interface SessionContext {

    UUID getSessionId();

    int nextMsgId();

    void onDeviceProfileUpdate(TransportProtos.SessionInfoProto sessionInfo, DeviceProfile deviceProfile);

    void onDeviceUpdate(TransportProtos.SessionInfoProto sessionInfo, Device device, Optional<DeviceProfile> deviceProfileOpt);
}
