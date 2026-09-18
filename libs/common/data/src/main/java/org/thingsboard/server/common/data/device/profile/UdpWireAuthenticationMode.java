package org.thingsboard.server.common.data.device.profile;
/**
 * Udp 链路上是否要求设备发送访问令牌（与 Core 侧是否仍校验设备身份无关）。
 * <ul>
 * <li>{@link #NONE}：设备连上后按分帧规则<strong>直接发业务数据</strong>，链路上不发送 token。SERVER 需在设备上配置
 * {@link org.thingsboard.server.common.data.device.data.UdpDeviceTransportConfiguration#getSourceHost() sourceHost}
 * 与对端 IP 绑定，由传输在 Core 侧静默建会话；CLIENT 模式下平台建连后也不向设备侧发鉴权帧。</li>
 * <li>{@link #DEFERRED_PAYLOAD_DEVICE_ID}：<strong>延迟注册（协议设备 ID）</strong>。负载中某 JSON 字段为<strong>协议侧设备标识</strong>（非 TB 访问令牌）；
 * 按设备传输配置中的 {@link org.thingsboard.server.common.data.device.data.UdpDeviceTransportConfiguration#getUdpWireAuthPayloadDeviceId() UdpWireAuthPayloadDeviceId}
 * 在<strong>租户内</strong>查找唯一设备，再使用其 {@code ACCESS_TOKEN} 向 Core 注册会话。该协议设备 ID 须在租户内唯一。</li>
 * </ul>
 */
public enum UdpWireAuthenticationMode {
    NONE,
    DEFERRED_PAYLOAD_DEVICE_ID
}