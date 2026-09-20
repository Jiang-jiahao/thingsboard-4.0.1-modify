package com.jnks.iot.server.transport.lwm2m.server.session;

import com.jnks.iot.server.gen.transport.TransportProtos;

public interface LwM2MSessionManager {

    void register(TransportProtos.SessionInfoProto sessionInfo);

    void deregister(TransportProtos.SessionInfoProto sessionInfo);


}
