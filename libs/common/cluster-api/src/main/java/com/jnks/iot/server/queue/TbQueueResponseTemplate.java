package com.jnks.iot.server.queue;

import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;

import java.util.Set;

public interface TbQueueResponseTemplate<Request extends TbQueueMsg, Response extends TbQueueMsg> {

    void subscribe();

    void subscribe(Set<TopicPartitionInfo> partitions);

    void launch(TbQueueHandler<Request, Response> handler);

    void stop();
}
