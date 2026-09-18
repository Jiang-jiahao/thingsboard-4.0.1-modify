package org.thingsboard.server.common.data.device.profile;
/**
 * TCP 设备配置中的历史枚举：UTF-8/ASCII 无 {@code method} 上行已统一为「单一遥测键」（见 {@link TcpDeviceProfileTransportConfiguration#getTcpOpaqueRuleEngineKey()}）。
 * 本枚举仍用于 JSON 反序列化兼容。
 */
public enum TcpJsonWithoutMethodMode {
    TELEMETRY_FLAT,
    OPAQUE_FOR_RULE_ENGINE
}