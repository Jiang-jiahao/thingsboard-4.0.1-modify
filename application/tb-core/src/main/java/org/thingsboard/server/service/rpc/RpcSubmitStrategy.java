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
package org.thingsboard.server.service.rpc;

import java.util.Arrays;

/**
 * 设备 RPC 提交策略。
 * <p>
 * 由设备档案配置，决定 Device Actor 如何向传输层下发待发送的 RPC 队列。
 */
public enum RpcSubmitStrategy {

    /** 一次性突发提交全部待发送请求。 */
    BURST,
    /** 收到设备 ACK 后再提交下一条。 */
    SEQUENTIAL_ON_ACK_FROM_DEVICE,
    /** 收到设备业务响应后再提交下一条。 */
    SEQUENTIAL_ON_RESPONSE_FROM_DEVICE;

    /**
     * 按名称解析策略，未知值回退为 {@link #BURST}。
     */
    public static RpcSubmitStrategy parse(String strategyStr) {
        return Arrays.stream(RpcSubmitStrategy.values())
                .filter(strategy -> strategy.name().equalsIgnoreCase(strategyStr))
                .findFirst()
                .orElse(BURST);
    }
}
