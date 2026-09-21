package com.jnks.iot.server.queue.memory;

import lombok.Data;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsg;
import com.jnks.iot.server.queue.JnksIotQueueProducer;

@Data
public class InMemoryJnksIotQueueProducer<T extends JnksIotQueueMsg> implements JnksIotQueueProducer<T> {

    private final InMemoryStorage storage;

    private final String defaultTopic;

    public InMemoryJnksIotQueueProducer(InMemoryStorage storage, String defaultTopic) {
        this.storage = storage;
        this.defaultTopic = defaultTopic;
    }

    @Override
    public void init() {

    }

    @Override
    public void send(TopicPartitionInfo tpi, T msg, JnksIotQueueCallback callback) {
        boolean result = storage.put(tpi.getFullTopicName(), msg);
        if (result) {
            if (callback != null) {
                callback.onSuccess(null);
            }
        } else {
            if (callback != null) {
                callback.onFailure(new RuntimeException("Failure add msg to InMemoryQueue"));
            }
        }
    }

    @Override
    public void stop() {

    }
}
