package com.jnks.iot.server.transport.lwm2m.server.store;


import com.jnks.iot.server.transport.lwm2m.secure.JnksIotX509DtlsSessionInfo;

public interface JnksIotLwM2MDtlsSessionStore {

    void put(String endpoint, JnksIotX509DtlsSessionInfo msg);

    JnksIotX509DtlsSessionInfo get(String endpoint);

    void remove(String endpoint);

}
