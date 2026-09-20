package com.jnks.iot.server.queue.environment;

public interface DistributedLockService {

    DistributedLock getLock(String key);

}
