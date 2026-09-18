package org.thingsboard.monitoring.config;

import java.util.List;

public interface MonitoringConfig<T extends MonitoringTarget> {

    List<T> getTargets();

}
