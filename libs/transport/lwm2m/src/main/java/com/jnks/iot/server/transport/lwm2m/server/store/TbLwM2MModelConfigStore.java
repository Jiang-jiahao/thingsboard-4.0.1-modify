package com.jnks.iot.server.transport.lwm2m.server.store;

import com.jnks.iot.server.transport.lwm2m.server.model.LwM2MModelConfig;

import java.util.List;

public interface TbLwM2MModelConfigStore {
    List<LwM2MModelConfig> getAll();

    void put(LwM2MModelConfig modelConfig);

    void remove(String endpoint);
}
