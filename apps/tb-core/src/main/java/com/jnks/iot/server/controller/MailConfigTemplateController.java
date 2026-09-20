package com.jnks.iot.server.controller;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.config.annotations.ApiOperation;
import com.jnks.iot.server.service.mail.TbMailConfigTemplateService;
import com.jnks.iot.server.service.security.permission.Operation;
import com.jnks.iot.server.service.security.permission.Resource;

import java.io.IOException;

import static com.jnks.iot.server.controller.ControllerConstants.SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH;

/**
 * 邮件服务器配置模板 REST 入口。
 * <p>
 * 返回各邮件服务商的默认 SMTP 模板，供管理后台「邮件设置」表单预填。
 * 仅在 tb-core 模块 中生效。
 * <p>
 * <b>URL 前缀：</b>{@code /api/mail/config/template}。
 * <p>
 * <b>权限：</b>SYS_ADMIN、TENANT_ADMIN；并校验 {@code ADMIN_SETTINGS} 的 READ。
 * <p>
 * <b>下游：</b>{@link TbMailConfigTemplateService}。
 *
 * @see TbMailConfigTemplateService
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/mail/config/template")
@Slf4j
public class MailConfigTemplateController extends BaseController {
    private static final String MAIL_CONFIG_TEMPLATE_DEFINITION = "Mail configuration template is set of default smtp settings for mail server that specific provider supports";
    private final TbMailConfigTemplateService mailConfigTemplateService;

    /**
     * 列出全部邮件配置模板（各服务商默认 SMTP 参数）。
     */
    @ApiOperation(value = "Get the list of all OAuth2 client registration templates (getClientRegistrationTemplates)" + SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH,
            notes = MAIL_CONFIG_TEMPLATE_DEFINITION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public JsonNode getClientRegistrationTemplates() throws JnksIotException, IOException {
        accessControlService.checkPermission(getCurrentUser(), Resource.ADMIN_SETTINGS, Operation.READ);
        return mailConfigTemplateService.findAllMailConfigTemplates();
    }

}
