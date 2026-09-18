package org.thingsboard.server.common.data.device.profile;

/**
 * 帧内字节与字段上配置的 {@code fixedWireIntegralValue} / {@code fixedBytesHex} 不一致。
 * 在命令规则解析路径上会终止该规则并尝试下一条；默认字段路径仍仅跳过该字段。
 */
public final class TcpHexFixedFieldMismatchException extends IllegalArgumentException {

    public TcpHexFixedFieldMismatchException(String message) {
        super(message);
    }
}
