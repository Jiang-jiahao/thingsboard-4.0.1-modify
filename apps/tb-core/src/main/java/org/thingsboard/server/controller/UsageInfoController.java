package org.thingsboard.server.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.thingsboard.server.common.data.UsageInfo;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.dao.usage.UsageInfoService;
/**
 * 当前租户资源用量统计 REST 入口。
 * <p>
 * 仅在 tb-core 模块（Core / Monolith）中生效。对照租户配置中的配额给出已用量。
 */
@RestController
@RequestMapping("/api")
@Slf4j
public class UsageInfoController extends BaseController {

    @Autowired
    private UsageInfoService usageInfoService;

    /**
     * 返回当前租户的设备、资产、用户、消息等用量统计。
     */
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/usage", method = RequestMethod.GET)
    @ResponseBody
    public UsageInfo getTenantUsageInfo() throws ThingsboardException {
        return checkNotNull(usageInfoService.getUsageInfo(getCurrentUser().getTenantId()));
    }
}
