package com.jnks.iot.server.queue.discovery.event;

import lombok.Getter;
import com.jnks.iot.server.queue.discovery.QueueKey;

import java.util.Set;

public class ClusterTopologyChangeEvent extends JnksIotApplicationEvent {

    private static final long serialVersionUID = -2441739930040282254L;

    @Getter
    private final Set<QueueKey> queueKeys;

    public ClusterTopologyChangeEvent(Object source, Set<QueueKey> queueKeys) {
        super(source);
        this.queueKeys = queueKeys;
    }
}
