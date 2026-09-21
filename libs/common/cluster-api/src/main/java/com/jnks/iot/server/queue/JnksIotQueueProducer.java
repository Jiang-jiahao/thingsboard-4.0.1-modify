package com.jnks.iot.server.queue;

import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;

public interface JnksIotQueueProducer<T extends JnksIotQueueMsg> {

    void init();

    String getDefaultTopic();

    public void send(TopicPartitionInfo tpi, T msg, JnksIotQueueCallback callback);

    default void flush() {
    }

    void stop();
}
