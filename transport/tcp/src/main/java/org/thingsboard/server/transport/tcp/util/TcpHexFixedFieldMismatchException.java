/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.tcp.util;

/**
 * 帧内字节与字段上配置的 {@code fixedWireIntegralValue} / {@code fixedBytesHex} 不一致。
 * 在命令规则解析路径上会终止该规则并尝试下一条；默认字段路径仍仅跳过该字段。
 */
public final class TcpHexFixedFieldMismatchException extends IllegalArgumentException {

    public TcpHexFixedFieldMismatchException(String message) {
        super(message);
    }
}
