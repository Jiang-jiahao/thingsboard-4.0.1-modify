package com.jnks.iot.server.service.subscription;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class JnksIotEntityUpdatesInfo {

    volatile long attributesUpdateTs;
    volatile long timeSeriesUpdateTs;

    public JnksIotEntityUpdatesInfo(long ts) {
        this.attributesUpdateTs = ts;
        this.timeSeriesUpdateTs = ts;
    }
}
