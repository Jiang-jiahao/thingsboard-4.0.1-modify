package org.thingsboard.server.queue;

import org.thingsboard.server.common.data.queue.Queue;

import java.util.List;

public interface TbQueueClusterService {

    void onQueuesUpdate(List<Queue> queues);

    void onQueuesDelete(List<Queue> queues);

}
