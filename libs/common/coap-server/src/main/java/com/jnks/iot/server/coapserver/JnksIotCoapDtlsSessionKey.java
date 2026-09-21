package com.jnks.iot.server.coapserver;

import java.net.InetSocketAddress;
import java.util.Objects;

public record JnksIotCoapDtlsSessionKey(InetSocketAddress peerAddress, String credentials) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JnksIotCoapDtlsSessionKey that = (JnksIotCoapDtlsSessionKey) o;
        return Objects.equals(peerAddress, that.peerAddress) &&
                Objects.equals(credentials, that.credentials);
    }
}

