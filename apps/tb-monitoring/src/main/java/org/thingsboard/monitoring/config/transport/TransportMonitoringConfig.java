package org.thingsboard.monitoring.config.transport;

import lombok.Data;
import org.thingsboard.monitoring.config.MonitoringConfig;

import java.util.List;

@Data
public abstract class TransportMonitoringConfig implements MonitoringConfig<TransportMonitoringTarget> {

    private List<TransportMonitoringTarget> targets;
    private int requestTimeoutMs;

    public abstract TransportType getTransportType();

}
