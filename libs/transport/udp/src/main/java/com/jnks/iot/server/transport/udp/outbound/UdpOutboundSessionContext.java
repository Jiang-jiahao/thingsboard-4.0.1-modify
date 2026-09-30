package com.jnks.iot.server.transport.udp.outbound;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionInfoProto;

/**
 * UDP 服务端模式下、设备还**没有真实会话**时给它建的"出站会话"。
 * <p>
 * 和设备上那张"虚拟会话"表里的其它实现（HTTP 的 {@code HttpOutboundSessionContext}）同一个套路：
 * 注册到 Core 只是为了**让 Core 有地方路由下发的 RPC**，不代表设备在线 ——
 * Core 判活跃只看 {@code lastActivityTime}（有没有收到数据），建会话不刷新它。
 */
@Data
@Builder
public class UdpOutboundSessionContext {

    private TenantId tenantId;
    private Device device;
    private DeviceProfile deviceProfile;
    private String token;
    private SessionInfoProto sessionInfo;
    private UdpOutboundTransportContext transportContext;

    public DeviceId getDeviceId() {
        return device.getId();
    }
}
