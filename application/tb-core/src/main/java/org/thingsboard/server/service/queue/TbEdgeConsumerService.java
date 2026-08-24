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

/**
 * Edge 队列消费服务标记接口。
 * <p>
 * 实现类在 Core 节点消费 {@code ToEdgeMsg} 与 Edge 通知，将实体变更推送到边缘网关会话。
 *
 * @see DefaultTbEdgeConsumerService
 */
public interface TbEdgeConsumerService {
}
