package org.thingsboard.server.service.queue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.dao.queue.QueueService;
import org.thingsboard.server.queue.discovery.QueueRoutingInfo;
import org.thingsboard.server.queue.discovery.QueueRoutingInfoService;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 队列路由信息提供者。
 * <p>
 * 分区发现依赖本服务从 DAO 加载全部规则引擎队列，组装 {@link QueueRoutingInfo}，供 Cluster 路由计算分区。
 *
 * @see QueueRoutingInfoService
 */
@Slf4j
@Service
public class DefaultQueueRoutingInfoService implements QueueRoutingInfoService {

    private final QueueService queueService;

    public DefaultQueueRoutingInfoService(QueueService queueService) {
        this.queueService = queueService;
    }

    /**
     * 查询全部队列并转换为路由信息列表。
     */
    @Override
    public List<QueueRoutingInfo> getAllQueuesRoutingInfo() {
        return queueService.findAllQueues().stream().map(QueueRoutingInfo::new).collect(Collectors.toList());
    }

}
