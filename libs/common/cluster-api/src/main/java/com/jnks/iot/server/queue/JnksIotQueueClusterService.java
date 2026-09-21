package com.jnks.iot.server.queue;

import com.jnks.iot.server.common.data.queue.Queue;

import java.util.List;

public interface JnksIotQueueClusterService {

    void onQueuesUpdate(List<Queue> queues);

    void onQueuesDelete(List<Queue> queues);

}
