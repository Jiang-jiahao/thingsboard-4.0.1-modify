package com.jnks.iot.server.service.queue;

import org.springframework.context.ApplicationListener;
import com.jnks.iot.server.queue.discovery.event.PartitionChangeEvent;

public interface JnksIotRuleEngineConsumerService extends ApplicationListener<PartitionChangeEvent> {

}
