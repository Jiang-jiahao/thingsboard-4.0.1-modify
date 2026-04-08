/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.service.protocoltemplate;

import lombok.Data;

/**
 * 下行组帧结果：小写连续十六进制字符串。
 */
@Data
public class ProtocolTemplateHexBuildResult {

    private boolean success;
    private String hex;
    private String errorMessage;
}
