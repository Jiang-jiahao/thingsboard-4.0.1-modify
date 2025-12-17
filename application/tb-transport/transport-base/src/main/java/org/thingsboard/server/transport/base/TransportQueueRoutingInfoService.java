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
package org.thingsboard.server.transport.base;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.gen.transport.TransportProtos.GetAllQueueRoutingInfoRequestMsg;
import org.thingsboard.server.queue.discovery.QueueRoutingInfo;
import org.thingsboard.server.queue.discovery.QueueRoutingInfoService;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 在传输服务启动时初始化队列路由信息
 */
@Slf4j
@Service
public class TransportQueueRoutingInfoService implements QueueRoutingInfoService {

    private final TransportService transportService;

    public TransportQueueRoutingInfoService(@Lazy TransportService transportService) {
        this.transportService = transportService;
    }

    @Override
    public List<QueueRoutingInfo> getAllQueuesRoutingInfo() {
        GetAllQueueRoutingInfoRequestMsg msg = GetAllQueueRoutingInfoRequestMsg.newBuilder().build();
        return transportService.getQueueRoutingInfo(msg).stream().map(QueueRoutingInfo::new).collect(Collectors.toList());
    }
}
