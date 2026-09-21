package com.jnks.iot.server.coapserver;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Data
public class JnksIotCoapDtlsSessionInMemoryStorage {

    private final ConcurrentMap<JnksIotCoapDtlsSessionKey, JnksIotCoapDtlsSessionInfo> dtlsSessionsMap = new ConcurrentHashMap<>();
    private long dtlsSessionInactivityTimeout;
    private long dtlsSessionReportTimeout;


    public JnksIotCoapDtlsSessionInMemoryStorage(long dtlsSessionInactivityTimeout, long dtlsSessionReportTimeout) {
        this.dtlsSessionInactivityTimeout = dtlsSessionInactivityTimeout;
        this.dtlsSessionReportTimeout = dtlsSessionReportTimeout;
    }

    public void put(JnksIotCoapDtlsSessionKey jnksIotCoapDtlsSessionKey, JnksIotCoapDtlsSessionInfo dtlsSessionInfo) {
        log.trace("DTLS session added to in-memory store: [{}] timestamp: [{}]", jnksIotCoapDtlsSessionKey, dtlsSessionInfo.getLastActivityTime());
        dtlsSessionsMap.putIfAbsent(jnksIotCoapDtlsSessionKey, dtlsSessionInfo);
    }

    public void evictTimeoutSessions() {
        long expTime = System.currentTimeMillis() - dtlsSessionInactivityTimeout;
        dtlsSessionsMap.entrySet().removeIf(entry -> {
            if (entry.getValue().getLastActivityTime() < expTime) {
                log.trace("DTLS session was removed from in-memory store: [{}]", entry.getKey());
                return true;
            } else {
                return false;
            }
        });
    }

}