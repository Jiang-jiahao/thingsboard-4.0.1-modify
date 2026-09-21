package com.jnks.iot.server.queue.memory;

import com.jnks.iot.server.queue.JnksIotQueueMsg;

import java.util.List;

public interface InMemoryStorage {

    void printStats();

    int getLagTotal();

    int getLag(String topic);

    boolean put(String topic, JnksIotQueueMsg msg);

    <T extends JnksIotQueueMsg> List<T> get(String topic) throws InterruptedException;

}
