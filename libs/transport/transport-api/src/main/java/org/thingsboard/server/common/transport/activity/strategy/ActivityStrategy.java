package org.thingsboard.server.common.transport.activity.strategy;

public interface ActivityStrategy {

    boolean onActivity();

    boolean onReportingPeriodEnd();

}
