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
