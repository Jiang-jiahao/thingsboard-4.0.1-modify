package org.thingsboard.server.vc.service;

import org.springframework.stereotype.Service;
import org.thingsboard.server.queue.discovery.QueueRoutingInfo;
import org.thingsboard.server.queue.discovery.QueueRoutingInfoService;

import java.util.Collections;
import java.util.List;

/**
 * 在版本控制启动时初始化队列路由信息
 */
@Service
public class VersionControlQueueRoutingInfoService implements QueueRoutingInfoService {
    @Override
    public List<QueueRoutingInfo> getAllQueuesRoutingInfo() {
        return Collections.emptyList();
    }
}
