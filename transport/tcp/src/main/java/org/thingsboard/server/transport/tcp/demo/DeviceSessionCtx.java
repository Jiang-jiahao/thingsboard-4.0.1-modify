//package org.thingsboard.server.transport.tcp.session;
//
//import io.netty.channel.ChannelHandlerContext;
//import lombok.Data;
//import lombok.extern.slf4j.Slf4j;
//import org.thingsboard.server.common.data.id.DeviceId;
//import org.thingsboard.server.common.data.id.TenantId;
//import org.thingsboard.server.common.transport.auth.TransportDeviceInfo;
//import org.thingsboard.server.gen.transport.TransportProtos;
//import org.thingsboard.server.transport.tcp.TcpTransportContext;
//
//import java.util.UUID;
//import java.util.concurrent.atomic.AtomicLong;
//
///**
// * 设备会话上下文，保存单个TCP连接的设备状态和会话信息
// */
//@Data
//@Slf4j
//public class DeviceSessionCtx {
//    // 会话基础信息
//    private final UUID sessionId;
//    private final long connectTime;
//    private final AtomicLong msgIdGenerator = new AtomicLong(0);
//
//    // 网络通道
//    private ChannelHandlerContext channel;
//    private volatile boolean connected = true;
//
//    // 设备认证信息
//    private volatile boolean authenticated = false;
//    private volatile DeviceId deviceId;
//    private volatile TenantId tenantId;
//    private volatile TransportDeviceInfo deviceInfo;
//    private volatile TransportProtos.SessionInfoProto sessionInfo;
//
//    // 传输上下文
//    private final TcpTransportContext transportContext;
//
//    // 会话统计
//    private final AtomicLong telemetryCount = new AtomicLong(0);
//    private final AtomicLong attributeCount = new AtomicLong(0);
//    private final AtomicLong rpcCount = new AtomicLong(0);
//    private volatile long lastActivityTime;
//
//    public DeviceSessionCtx(UUID sessionId, TcpTransportContext transportContext) {
//        this.sessionId = sessionId;
//        this.transportContext = transportContext;
//        this.connectTime = System.currentTimeMillis();
//        this.lastActivityTime = connectTime;
//    }
//
//    /**
//     * 设置设备信息（认证成功后调用）
//     */
//    public void setDeviceInfo(TransportDeviceInfo deviceInfo) {
//        this.deviceInfo = deviceInfo;
//        this.deviceId = deviceInfo != null ? deviceInfo.getDeviceId() : null;
//        this.tenantId = deviceInfo != null ? deviceInfo.getTenantId() : null;
//        this.authenticated = deviceInfo != null;
//
//        if (log.isDebugEnabled()) {
//            log.debug("[{}] 设备会话上下文已更新，设备: {}", sessionId,
//                     deviceId != null ? deviceId.getId() : "null");
//        }
//    }
//
//    /**
//     * 生成唯一消息ID
//     */
//    public int nextMsgId() {
//        return (int) (msgIdGenerator.incrementAndGet() & 0x7FFFFFFF);
//    }
//
//    /**
//     * 更新最后活动时间
//     */
//    public void updateActivityTime() {
//        this.lastActivityTime = System.currentTimeMillis();
//    }
//
//    /**
//     * 获取会话空闲时间（毫秒）
//     */
//    public long getIdleTime() {
//        return System.currentTimeMillis() - lastActivityTime;
//    }
//
//    /**
//     * 检查会话是否有效
//     */
//    public boolean isValid() {
//        return connected && channel != null && channel.channel().isActive();
//    }
//
//    /**
//     * 关闭会话
//     */
//    public void close() {
//        this.connected = false;
//        this.authenticated = false;
//
//        if (channel != null && channel.channel().isActive()) {
//            channel.close();
//        }
//
//        log.debug("[{}] 设备会话已关闭", sessionId);
//    }
//
//    /**
//     * 记录遥测数据统计
//     */
//    public void incrementTelemetryCount() {
//        telemetryCount.incrementAndGet();
//        updateActivityTime();
//    }
//
//    /**
//     * 记录属性数据统计
//     */
//    public void incrementAttributeCount() {
//        attributeCount.incrementAndGet();
//        updateActivityTime();
//    }
//
//    /**
//     * 记录RPC统计
//     */
//    public void incrementRpcCount() {
//        rpcCount.incrementAndGet();
//        updateActivityTime();
//    }
//}