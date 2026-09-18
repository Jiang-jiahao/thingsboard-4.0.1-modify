package org.thingsboard.server.transport.mqtt.limits;

import lombok.Data;

/**
 * 网关会话限制对象，用于存储网关会话的速率限制信息，返回给设备端（处理设备端用于获取会话限制的rpc）。
 */
@Data
public class GatewaySessionLimits extends SessionLimits {

    private SessionRateLimits gatewayRateLimits;

}
