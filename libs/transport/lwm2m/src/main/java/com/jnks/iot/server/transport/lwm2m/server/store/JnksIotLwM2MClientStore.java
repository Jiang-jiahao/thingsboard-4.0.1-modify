package com.jnks.iot.server.transport.lwm2m.server.store;

import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;

import java.util.Set;

public interface JnksIotLwM2MClientStore {

    LwM2mClient get(String endpoint);

    Set<LwM2mClient> getAll();

    void put(LwM2mClient client);

    void remove(String endpoint);
}
