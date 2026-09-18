package org.thingsboard.monitoring.config.transport;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.thingsboard.monitoring.config.MonitoringTarget;

import java.util.UUID;

@Data
public class TransportMonitoringTarget implements MonitoringTarget {

    private String baseUrl;
    private DeviceConfig device; // set manually during initialization
    private String queue;
    private boolean checkDomainIps;

    @Override
    public UUID getDeviceId() {
        return device.getId();
    }

    public String getQueue() {
        return StringUtils.defaultIfEmpty(queue, "Main");
    }

}
