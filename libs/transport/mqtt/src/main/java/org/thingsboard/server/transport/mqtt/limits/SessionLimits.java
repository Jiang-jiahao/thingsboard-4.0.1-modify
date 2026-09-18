package org.thingsboard.server.transport.mqtt.limits;

import lombok.Data;
import org.thingsboard.server.common.data.TransportPayloadType;

/**
 * 会话限制对象，用于存储会话的速率限制信息，返回给设备端（处理设备端用于获取会话限制的rpc）。
 */
@Data
public class SessionLimits {

    private int maxPayloadSize;
    private int maxInflightMessages;
    private SessionRateLimits rateLimits;

    public record SessionRateLimits(String messages, String telemetryMessages, String telemetryDataPoints) {}
}
