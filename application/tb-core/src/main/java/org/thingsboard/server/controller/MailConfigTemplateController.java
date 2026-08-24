/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.controller;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.mail.TbMailConfigTemplateService;
import org.thingsboard.server.service.security.permission.Operation;
import org.thingsboard.server.service.security.permission.Resource;

import java.io.IOException;

import static org.thingsboard.server.controller.ControllerConstants.SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH;

/**
 * 邮件服务器配置模板 REST 入口。
 * <p>
 * 返回各邮件服务商的默认 SMTP 模板，供管理后台「邮件设置」表单预填。
 * 仅在 {@link TbCoreComponent} 中生效。
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
@TbCoreComponent
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
    public JsonNode getClientRegistrationTemplates() throws ThingsboardException, IOException {
        accessControlService.checkPermission(getCurrentUser(), Resource.ADMIN_SETTINGS, Operation.READ);
        return mailConfigTemplateService.findAllMailConfigTemplates();
    }

}
