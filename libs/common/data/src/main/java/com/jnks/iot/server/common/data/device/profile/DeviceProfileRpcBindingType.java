package com.jnks.iot.server.common.data.device.profile;

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
    HTTP_OUTBOUND,
    /**
     * MQTT 自定义数据格式：按档案配置的请求/响应主题与 payload 模板下发（MQTT 服务端与 MQTT Pull 客户端均可用）。
     */
    MQTT_CUSTOM,
    /**
     * TCP/UDP 自定义 JSON：{@code params} 即负载，按 UTF-8 原样发出，<strong>不包 {@code {method,requestId,...}} 信封</strong>。
     * 因为没有 requestId、与设备响应无法对应，只能单向（{@code oneWay} 必须为 true）。
     */
    CUSTOM_JSON
}
