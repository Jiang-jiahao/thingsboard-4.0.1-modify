package com.jnks.iot.server.transport.lwm2m.server.store;

import com.jnks.iot.server.transport.lwm2m.server.model.LwM2MModelConfig;

import java.util.Collections;
import java.util.List;

public class JnksIotDummyLwM2MModelConfigStore implements JnksIotLwM2MModelConfigStore {
    @Override
    public List<LwM2MModelConfig> getAll() {
        return Collections.emptyList();
    }

    @Override
    public void put(LwM2MModelConfig modelConfig) {

    }

    @Override
    public void remove(String endpoint) {

    }
}
