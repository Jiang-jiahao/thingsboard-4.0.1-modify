package org.thingsboard.server.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.config.annotations.ApiOperation;
/**
 * UI 帮助资源基址 REST 入口。
 * <p>
 * 仅在 tb-core 模块（Core / Monolith）中生效。前端据此拼接帮助文档 URL。
 */
@RestController
@RequestMapping("/api")
public class UiSettingsController extends BaseController {

    @Value("${ui.help.base-url}")
    private String helpBaseUrl;

    /**
     * 返回配置文件中的 UI 帮助文档基址 {@code ui.help.base-url}。
     */
    @ApiOperation(value = "Get UI help base url (getHelpBaseUrl)",
            notes = "Get UI help base url used to fetch help assets. " +
                    "The actual value of the base url is configurable in the system configuration file.")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/uiSettings/helpBaseUrl", method = RequestMethod.GET)
    @ResponseBody
    public String getHelpBaseUrl() throws ThingsboardException {
        return helpBaseUrl;
    }

}
