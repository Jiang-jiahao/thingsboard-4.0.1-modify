package com.jnks.iot.server.common.transport.activity.strategy;

public interface ActivityStrategy {

    boolean onActivity();

    boolean onReportingPeriodEnd();

}
