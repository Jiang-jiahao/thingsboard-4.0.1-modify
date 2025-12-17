package org.thingsboard.server.transport.tcp.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonObject;
import org.thingsboard.common.util.JacksonUtil;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TcpMessage {
    
    public enum TcpMessageType {
        CONNECT(0x01),        // 连接请求
        CONNACK(0x02),        // 连接确认
        TELEMETRY(0x03),      // 遥测数据
        ATTRIBUTES(0x04),     // 属性数据
        RPC_REQUEST(0x05),    // RPC请求
        RPC_RESPONSE(0x06),   // RPC响应
        PING(0x07),          // 心跳请求
        PONG(0x08),          // 心跳响应
        DISCONNECT(0x09),     // 断开连接
        DISCONNACK(0x0A),     // 断开确认
        ACK(0x0B),           // 确认
        ERROR(0x0C);         // 错误
        
        private final byte value;
        
        TcpMessageType(int value) {
            this.value = (byte) value;
        }
        
        public byte getValue() {
            return value;
        }
        
        public static TcpMessageType fromValue(byte value) {
            for (TcpMessageType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown message type: " + value);
        }
    }
    
    private TcpMessageType type;
    private int messageId;
    private byte[] payload;
    
    private TcpMessage() {}
    
    public TcpMessage(TcpMessageType type, int messageId, byte[] payload) {
        this.type = type;
        this.messageId = messageId;
        this.payload = payload;
    }
    
    public byte[] encode() {
        int payloadLength = payload != null ? payload.length : 0;
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + payloadLength);
        
        buffer.put(type.getValue());              // 消息类型 (1 byte)
        buffer.putInt(messageId);                 // 消息ID (4 bytes)
        buffer.putInt(payloadLength);             // 载荷长度 (4 bytes)
        
        if (payloadLength > 0) {
            buffer.put(payload);                  // 载荷内容
        }
        
        return buffer.array();
    }
    
    public static TcpMessage decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        byte typeByte = buffer.get();
        int messageId = buffer.getInt();
        int payloadLength = buffer.getInt();
        
        TcpMessageType type = TcpMessageType.fromValue(typeByte);
        byte[] payload = null;
        
        if (payloadLength > 0 && buffer.remaining() >= payloadLength) {
            payload = new byte[payloadLength];
            buffer.get(payload);
        }
        
        return new TcpMessage(type, messageId, payload);
    }
    
    public static TcpMessage success(TcpMessageType type, int messageId) {
        JsonObject response = new JsonObject();
        response.addProperty("status", "SUCCESS");
        return new TcpMessage(type, messageId, 
            response.toString().getBytes(StandardCharsets.UTF_8));
    }
    
    public static TcpMessage success(TcpMessageType type, String message) {
        JsonObject response = new JsonObject();
        response.addProperty("status", "SUCCESS");
        response.addProperty("message", message);
        return new TcpMessage(type, 0, 
            response.toString().getBytes(StandardCharsets.UTF_8));
    }
    
    public static TcpMessage error(TcpMessageType type, int messageId, String error) {
        JsonObject response = new JsonObject();
        response.addProperty("status", "ERROR");
        response.addProperty("error", error);
        return new TcpMessage(type, messageId, 
            response.toString().getBytes(StandardCharsets.UTF_8));
    }
    
    public static TcpMessage create(TcpMessageType type, int messageId, Object payload) {
        byte[] payloadBytes = JacksonUtil.toString(payload).getBytes(StandardCharsets.UTF_8);
        return new TcpMessage(type, messageId, payloadBytes);
    }
    
    // Getters
    public TcpMessageType getType() {
        return type;
    }
    
    public int getMessageId() {
        return messageId;
    }
    
    public byte[] getPayload() {
        return payload;
    }
    
    public JsonNode getPayloadAsJson() {
        if (payload == null || payload.length == 0) {
            return null;
        }
        return JacksonUtil.fromBytes(payload, JsonNode.class);
    }
    
    public String getPayloadAsString() {
        if (payload == null) {
            return null;
        }
        return new String(payload, StandardCharsets.UTF_8);
    }
}