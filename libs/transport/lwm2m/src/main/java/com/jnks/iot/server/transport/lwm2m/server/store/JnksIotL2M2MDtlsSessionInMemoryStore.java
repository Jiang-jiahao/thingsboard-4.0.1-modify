package com.jnks.iot.server.transport.lwm2m.server.store;

import com.jnks.iot.server.transport.lwm2m.secure.JnksIotX509DtlsSessionInfo;

import java.util.concurrent.ConcurrentHashMap;

public class JnksIotL2M2MDtlsSessionInMemoryStore implements JnksIotLwM2MDtlsSessionStore {

    private final ConcurrentHashMap<String, JnksIotX509DtlsSessionInfo> store = new ConcurrentHashMap<>();

    @Override
    public void put(String endpoint, JnksIotX509DtlsSessionInfo msg) {
        store.put(endpoint, msg);
    }

    @Override
    public JnksIotX509DtlsSessionInfo get(String endpoint) {
        return store.get(endpoint);
    }

    @Override
    public void remove(String endpoint) {
        store.remove(endpoint);
    }
}
