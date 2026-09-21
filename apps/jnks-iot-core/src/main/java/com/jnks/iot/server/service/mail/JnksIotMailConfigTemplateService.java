package com.jnks.iot.server.service.mail;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/**
 * 邮件服务商配置模板查询接口。
 */
public interface JnksIotMailConfigTemplateService {
    /** 返回内置的邮件 OAuth2/SMTP 配置模板 JSON。 */
    JsonNode findAllMailConfigTemplates() throws IOException;
}
