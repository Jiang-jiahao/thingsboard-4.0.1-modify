package com.jnks.iot.server.queue;

import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;

import java.util.Set;

public interface JnksIotQueueResponseTemplate<Request extends JnksIotQueueMsg, Response extends JnksIotQueueMsg> {

    void subscribe();

    void subscribe(Set<TopicPartitionInfo> partitions);

    void launch(JnksIotQueueHandler<Request, Response> handler);

    void stop();
}
