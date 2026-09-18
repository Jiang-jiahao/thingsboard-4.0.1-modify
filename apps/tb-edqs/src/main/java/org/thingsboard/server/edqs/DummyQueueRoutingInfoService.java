package org.thingsboard.server.edqs;

import org.springframework.stereotype.Service;
import org.thingsboard.server.queue.discovery.QueueRoutingInfo;
import org.thingsboard.server.queue.discovery.QueueRoutingInfoService;

import java.util.Collections;
import java.util.List;

/**
 * 假的队列信息服务
 * 防止edqs服务启动报错
 */
@Service
public class DummyQueueRoutingInfoService implements QueueRoutingInfoService {

    @Override
    public List<QueueRoutingInfo> getAllQueuesRoutingInfo() {
        return Collections.emptyList();
    }

}
