package org.thingsboard.server.transport.tcp.util;

/**
 * TCP传输层消息类型枚举
 */
public enum TcpMessageType {
    // 连接相关
    CONNECT((byte) 0x01, "设备连接请求"),
    CONNACK((byte) 0x02, "连接确认"),
    
    // 数据上报
    TELEMETRY((byte) 0x03, "遥测数据上报"),
    ATTRIBUTES((byte) 0x04, "属性数据上报"),
    
    // RPC通信
    RPC_REQUEST((byte) 0x05, "下发RPC请求"),
    RPC_RESPONSE((byte) 0x06, "RPC响应"),
    
    // 连接维护
    PING((byte) 0x07, "心跳请求"),
    PONG((byte) 0x08, "心跳响应"),
    DISCONNECT((byte) 0x09, "断开连接"),
    DISCONNACK((byte) 0x0A, "断开确认"),
    
    // 通用
    ACK((byte) 0x0B, "操作成功确认"),
    ERROR((byte) 0x0C, "错误响应");

    private final byte value;
    private final String description;

    TcpMessageType(byte value, String description) {
        this.value = value;
        this.description = description;
    }

    public byte getValue() {
        return value;
    }

    public String getDescription() {
        return description;
    }

    /**
     * 根据字节值查找对应的消息类型
     */
    public static TcpMessageType fromValue(byte value) {
        for (TcpMessageType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("未知的消息类型: 0x" + String.format("%02X", value));
    }

    /**
     * 检查是否为有效的心跳消息类型
     */
    public boolean isHeartbeat() {
        return this == PING || this == PONG;
    }

    /**
     * 检查是否为连接控制消息
     */
    public boolean isConnectionControl() {
        return this == CONNECT || this == CONNACK || this == DISCONNECT || this == DISCONNACK;
    }
}