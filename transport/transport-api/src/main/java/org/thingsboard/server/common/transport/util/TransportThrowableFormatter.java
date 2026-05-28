/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.transport.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.thingsboard.server.common.data.StringUtils;

/**
 * 传输层写入设备 ERROR / LC 事件时的异常文案（{@code getRootCauseMessage} 对无 message 的异常常为空）。
 */
public final class TransportThrowableFormatter {

    private static final int MAX_ERROR_LENGTH = 8000;

    private TransportThrowableFormatter() {
    }

    public static String format(Throwable error) {
        if (error == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        Throwable current = error;
        int depth = 0;
        while (current != null && depth < 8) {
            if (depth > 0) {
                sb.append("Caused by: ");
            }
            sb.append(current.getClass().getName());
            if (StringUtils.isNotBlank(current.getMessage())) {
                sb.append(": ").append(current.getMessage());
            } else {
                sb.append(": (no message)");
            }
            sb.append('\n');
            current = current.getCause();
            depth++;
        }
        String stack = ExceptionUtils.getStackTrace(error);
        if (StringUtils.isNotBlank(stack)) {
            sb.append(stack);
        }
        return StringUtils.truncate(sb.toString().trim(), MAX_ERROR_LENGTH);
    }

    public static RuntimeException descriptiveError(String message) {
        return new RuntimeException(message);
    }
}
