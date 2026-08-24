/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.queue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.thingsboard.server.dao.queue.QueueService;
import org.thingsboard.server.queue.discovery.QueueRoutingInfo;
import org.thingsboard.server.queue.discovery.QueueRoutingInfoService;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 队列路由信息提供者（仅 monolith / tb-core 生效）。
 * <p>
 * 分区发现依赖本服务从 DAO 加载全部规则引擎队列，组装 {@link QueueRoutingInfo}，供 Cluster 路由计算分区。
 *
 * @see QueueRoutingInfoService
 */
@Slf4j
@Service
@ConditionalOnExpression("'${service.type:null}'=='monolith' || '${service.type:null}'=='tb-core'")
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
