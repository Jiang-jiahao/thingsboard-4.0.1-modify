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
package org.thingsboard.server.service.ruleengine;

import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * Core 经队列同步调用规则引擎的 REST 回调门面。
 * <p>
 * REST 把 {@link TbMsg} 推入规则引擎后登记本机回调；规则引擎处理完毕经 Core 通知队列
 * 把响应交回 {@link #onQueueMsg}，解除等待。超时则回调 {@code null}。
 *
 * @see DefaultRuleEngineCallService
 */
public interface RuleEngineCallService {

    /**
     * 将 REST 请求消息推入规则引擎并登记响应消费者。
     */
    void processRestApiCallToRuleEngine(TenantId tenantId, UUID requestId, TbMsg request, boolean useQueueFromTbMsg, Consumer<TbMsg> responseConsumer);

    /**
     * 处理规则引擎经通知队列返回的 REST 调用响应。
     */
    void onQueueMsg(TransportProtos.RestApiCallResponseMsgProto restApiCallResponseMsg, TbCallback callback);
}
