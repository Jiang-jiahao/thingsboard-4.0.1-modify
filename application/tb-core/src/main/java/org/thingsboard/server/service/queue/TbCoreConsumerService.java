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

import org.springframework.context.ApplicationListener;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;

/**
 * Core 主队列消费服务标记接口。
 * <p>
 * 实现类订阅本节点负责的 Core Topic 分区，将 {@code ToCoreMsg} / 通知消息分发到设备状态、订阅、RPC 等组件。
 * 分区变更通过 {@link PartitionChangeEvent} 驱动。
 *
 * @see DefaultTbCoreConsumerService
 */
public interface TbCoreConsumerService extends ApplicationListener<PartitionChangeEvent> {

}
