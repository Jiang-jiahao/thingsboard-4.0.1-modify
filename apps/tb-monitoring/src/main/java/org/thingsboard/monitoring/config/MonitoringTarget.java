package org.thingsboard.monitoring.config;

import java.util.UUID;

public interface MonitoringTarget {

    UUID getDeviceId();

    String getBaseUrl();

    default String getQueue() {
        return "Main";
    }

    boolean isCheckDomainIps();

}
