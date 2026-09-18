package org.thingsboard.server.service.security.auth.jwt.extractor;

import jakarta.servlet.http.HttpServletRequest;

/**
 * token信息提取器
 */
public interface TokenExtractor {
    String extract(HttpServletRequest request);
}