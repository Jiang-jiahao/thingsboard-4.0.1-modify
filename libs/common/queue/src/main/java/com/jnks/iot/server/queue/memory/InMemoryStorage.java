package com.jnks.iot.server.queue.memory;

import com.jnks.iot.server.queue.TbQueueMsg;

import java.util.List;

public interface InMemoryStorage {

    void printStats();

    int getLagTotal();

    int getLag(String topic);

    boolean put(String topic, TbQueueMsg msg);

    <T extends TbQueueMsg> List<T> get(String topic) throws InterruptedException;

}
