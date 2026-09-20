package com.jnks.iot.server.service.sync.vc;

import org.springframework.context.ApplicationListener;
import com.jnks.iot.server.queue.discovery.event.PartitionChangeEvent;

public interface ClusterVersionControlService extends ApplicationListener<PartitionChangeEvent> {
}
