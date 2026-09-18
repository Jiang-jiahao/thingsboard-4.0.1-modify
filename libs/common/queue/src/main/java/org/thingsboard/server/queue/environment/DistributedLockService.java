package org.thingsboard.server.queue.environment;

public interface DistributedLockService {

    DistributedLock getLock(String key);

}
