package org.thingsboard.server.queue.discovery;

import java.util.List;

/**
 * 队列路由信息服务
 */
public interface QueueRoutingInfoService {

    List<QueueRoutingInfo> getAllQueuesRoutingInfo();

}
