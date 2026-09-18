package org.thingsboard.server.common.data.device.profile;
/**
 * TCP 接入模式：平台监听端口由设备连接（SERVER），或由平台主动连接设备（CLIENT）。
 */
public enum TcpTransportConnectMode {
    SERVER,
    CLIENT
}