package org.thingsboard.server.queue.environment;

public interface DistributedLock {

    void lock();

    void unlock();

}
