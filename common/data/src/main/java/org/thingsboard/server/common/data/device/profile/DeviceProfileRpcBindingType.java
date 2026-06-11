/**
 * Copyright © 2016-2025 The Thingsboard Authors
 */
package org.thingsboard.server.common.data.device.profile;

/**
 * 设备档案 RPC 方法目录条目的线下发绑定类型。
 */
public enum DeviceProfileRpcBindingType {
    /**
     * TCP 协议模板：下发前由平台按模板下行命令组 HEX（{@code buildHex}），再以 {@code params.hex} 投递。
     */
    TCP_TEMPLATE,
    /**
     * UDP 协议模板：与 TCP_TEMPLATE 相同，经 UDP 传输下发 {@code params.hex} 原始字节。
     */
    UDP_TEMPLATE,
    /**
     * 原生 RPC：{@code method} / {@code params} 按设备固件约定透传（MQTT、HTTP 被动长轮询等）。
     */
    NATIVE,
    /**
     * HTTP 主动出站：平台按档案中配置的 HTTP 接口调用厂家服务端（与 HTTP Pull 共用鉴权）。
     */
    HTTP_OUTBOUND
}
