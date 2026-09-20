package com.jnks.iot.server.queue.environment;

public interface DistributedLock {

    void lock();

    void unlock();

}
