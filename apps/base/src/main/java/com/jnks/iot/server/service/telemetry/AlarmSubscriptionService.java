package com.jnks.iot.server.service.telemetry;

import org.springframework.context.ApplicationListener;
import com.jnks.iot.rule.engine.api.RuleEngineAlarmService;
import com.jnks.iot.server.queue.discovery.event.PartitionChangeEvent;

/**
 * Created by ashvayka on 27.03.18.
 */
public interface AlarmSubscriptionService extends RuleEngineAlarmService, ApplicationListener<PartitionChangeEvent> {

}
