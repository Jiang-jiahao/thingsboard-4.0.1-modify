package com.jnks.iot.server.queue;

import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;

import java.util.List;
import java.util.Set;

public interface JnksIotQueueConsumer<T extends JnksIotQueueMsg> {

    String getTopic();

    void subscribe();

    void subscribe(Set<TopicPartitionInfo> partitions);

    void stop();

    void unsubscribe();

    List<T> poll(long durationInMillis);

    void commit();

    boolean isStopped();

    List<String> getFullTopicNames();

}
