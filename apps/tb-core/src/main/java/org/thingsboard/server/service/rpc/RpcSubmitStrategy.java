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
