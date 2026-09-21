package com.jnks.iot.server.transport.lwm2m.server.store;

import com.jnks.iot.server.transport.lwm2m.server.ota.firmware.LwM2MClientFwOtaInfo;
import com.jnks.iot.server.transport.lwm2m.server.ota.software.LwM2MClientSwOtaInfo;

public class JnksIotDummyLwM2MClientOtaInfoStore implements JnksIotLwM2MClientOtaInfoStore {

    @Override
    public LwM2MClientFwOtaInfo getFw(String endpoint) {
        return null;
    }

    @Override
    public LwM2MClientSwOtaInfo getSw(String endpoint) {
        return null;
    }

    @Override
    public void putFw(LwM2MClientFwOtaInfo info) {

    }

    @Override
    public void putSw(LwM2MClientSwOtaInfo info) {

    }
}
