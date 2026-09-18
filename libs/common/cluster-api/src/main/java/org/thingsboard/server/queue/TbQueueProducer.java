package org.thingsboard.server.queue;

import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;

public interface TbQueueProducer<T extends TbQueueMsg> {

    void init();

    String getDefaultTopic();

    public void send(TopicPartitionInfo tpi, T msg, TbQueueCallback callback);

    default void flush() {
    }

    void stop();
}
