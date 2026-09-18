package org.thingsboard.server.service.mail;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;

import java.io.IOException;

/**
 * 邮件配置模板服务：启动时从 classpath 加载 {@code mail_config_templates.json} 并缓存。
 * <p>
 * <b>职责：</b>向管理界面提供各邮件服务商的 OAuth2/SMTP 配置模板。
 * <p>
 * <b>触发方式：</b>启动 {@code @PostConstruct} 加载；查询时直接返回缓存。
 */
@Service
@Slf4j
public class DefaultTbMailConfigTemplateService implements TbMailConfigTemplateService {

    private JsonNode mailConfigTemplates;

    @PostConstruct
    private void postConstruct() throws IOException {
        mailConfigTemplates = JacksonUtil.toJsonNode(new ClassPathResource("/templates/mail_config_templates.json").getInputStream());
    }

    /** 返回已加载的邮件配置模板。 */
    @Override
    public JsonNode findAllMailConfigTemplates() {
        return mailConfigTemplates;
    }
}
