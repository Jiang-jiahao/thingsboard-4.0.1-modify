package com.jnks.iot.server.transport.lwm2m.server.store;


import com.jnks.iot.server.transport.lwm2m.secure.TbX509DtlsSessionInfo;

public interface TbLwM2MDtlsSessionStore {

    void put(String endpoint, TbX509DtlsSessionInfo msg);

    TbX509DtlsSessionInfo get(String endpoint);

    void remove(String endpoint);

}
