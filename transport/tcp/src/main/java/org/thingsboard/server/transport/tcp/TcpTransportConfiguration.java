//package org.thingsboard.server.transport.tcp;
//
//import org.springframework.boot.context.properties.ConfigurationProperties;
//import org.springframework.context.annotation.Configuration;
//import lombok.Data;
//
//@Data
//@Configuration
//@ConfigurationProperties(prefix = "transport.tcp")
//public class TcpTransportConfiguration {
//
//    // 服务器配置
//    private String bindAddress = "0.0.0.0";
//    private int bindPort = 1884;
//    private int bossGroupThreadCount = 1;
//    private int workerGroupThreadCount = 8;
//    private boolean soKeepAlive = true;
//    private int soBacklog = 4096;
//
//    // SSL配置
//    private boolean sslEnabled = false;
//    private String sslProtocol = "TLS";
//    private String sslKeyStore;
//    private String sslKeyStorePassword;
//    private String sslKeyPassword;
//    private String sslTrustStore;
//    private String sslTrustStorePassword;
//
//    // 客户端配置
//    private boolean clientEnabled = false;
//    private long clientReconnectInterval = 30000;
//    private int maxReconnectAttempts = 10;
//
//    // 消息配置
//    private int maxPayloadSize = 65535;
//    private int messageQueueSizePerDeviceLimit = 100;
//    private long timeout = 10000;
//
//    // 性能配置
//    private int maxConnections = 10000;
//    private long connectionTimeout = 10000;
//    private int idleTimeout = 60;
//
//    // 速率限制
//    private boolean rateLimitsEnabled = true;
//    private int maxMessagesPerSecond = 1000;
//    private int maxTelemetryPerSecond = 100;
//    private int maxAttributesPerSecond = 50;
//}