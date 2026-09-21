package com.jnks.iot.server.controller;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.OAuth2ClientId;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundle;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundleInfo;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.config.annotations.ApiOperation;
import com.jnks.iot.server.service.entitiy.mobile.JnksIotMobileAppBundleService;
import com.jnks.iot.server.service.security.permission.Operation;
import com.jnks.iot.server.service.security.permission.Resource;

import java.util.List;
import java.util.UUID;

import static com.jnks.iot.server.controller.ControllerConstants.PAGE_NUMBER_DESCRIPTION;
import static com.jnks.iot.server.controller.ControllerConstants.PAGE_SIZE_DESCRIPTION;
import static com.jnks.iot.server.controller.ControllerConstants.SORT_ORDER_DESCRIPTION;
import static com.jnks.iot.server.controller.ControllerConstants.SORT_PROPERTY_DESCRIPTION;
import static com.jnks.iot.server.controller.ControllerConstants.SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH;
import static com.jnks.iot.server.controller.ControllerConstants.UUID_WIKI_LINK;

/**
 * 移动端应用包（Mobile App Bundle）REST 入口。
 * <p>
 * Bundle 把 Android / iOS 应用成对管理，并保存 OAuth2 客户端、自注册和布局配置。
 * 仅在 jnks-iot-core 模块 中生效。
 * <p>
 * <b>URL 前缀：</b>{@code /api}。路径如 {@code /mobile/bundle}、{@code /mobile/bundle/infos}、
 * {@code /mobile/bundle/{id}/oauth2Clients}。
 * <p>
 * <b>权限：</b>全部接口 SYS_ADMIN、TENANT_ADMIN。
 * <p>
 * <b>下游：</b>写路径 {@link JnksIotMobileAppBundleService}；查询走基类 {@code mobileAppBundleService}。
 *
 * @see JnksIotMobileAppBundleService
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class MobileAppBundleController extends BaseController {

    private final JnksIotMobileAppBundleService jnksIotMobileAppBundleService;

    /**
     * 创建或更新移动端 Bundle，并可同时绑定 OAuth2 客户端。
     */
    @ApiOperation(value = "Save Or update Mobile app bundle (saveMobileAppBundle)",
            notes = "Create or update the Mobile app bundle that represents tha pair of ANDROID and IOS app and " +
                    "mobile settings like oauth2 clients, self-registration and layout configuration." +
                    "When creating mobile app bundle, platform generates Mobile App Bundle Id as " + UUID_WIKI_LINK +
                    "The newly created Mobile App Bundle Id will be present in the response. " +
                    "Referencing non-existing Mobile App Bundle Id will cause 'Not Found' error."  + SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @PostMapping(value = "/mobile/bundle")
    public MobileAppBundle saveMobileAppBundle(
            @Parameter(description = "A JSON value representing the Mobile Application Bundle.", required = true)
            @RequestBody @Valid MobileAppBundle mobileAppBundle,
            @Parameter(description = "A list of oauth2 client ids, separated by comma ','", array = @ArraySchema(schema = @Schema(type = "string")))
            @RequestParam(name = "oauth2ClientIds", required = false) UUID[] ids) throws Exception {
        mobileAppBundle.setTenantId(getTenantId());
        checkEntity(mobileAppBundle.getId(), mobileAppBundle, Resource.MOBILE_APP_BUNDLE);
        return jnksIotMobileAppBundleService.save(mobileAppBundle, getOAuth2ClientIds(ids), getCurrentUser());
    }

    /**
     * 替换指定 Bundle 绑定的 OAuth2 客户端列表。
     */
    @ApiOperation(value = "Update oauth2 clients (updateOauth2Clients)",
            notes = "Update oauth2 clients of the specified mobile app bundle." + SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @PutMapping(value = "/mobile/bundle/{id}/oauth2Clients")
    public void updateOauth2Clients(@PathVariable UUID id,
                                    @RequestBody UUID[] clientIds) throws JnksIotException {
        MobileAppBundleId mobileAppBundleId = new MobileAppBundleId(id);
        MobileAppBundle mobileAppBundle = checkMobileAppBundleId(mobileAppBundleId, Operation.WRITE);
        List<OAuth2ClientId> oAuth2ClientIds = getOAuth2ClientIds(clientIds);
        jnksIotMobileAppBundleService.updateOauth2Clients(mobileAppBundle, oAuth2ClientIds, getCurrentUser());
    }

    /**
     * 分页列出当前租户的 Bundle 信息。
     */
    @ApiOperation(value = "Get mobile app bundle infos (getTenantMobileAppBundleInfos)", notes = SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @GetMapping(value = "/mobile/bundle/infos")
    public PageData<MobileAppBundleInfo> getTenantMobileAppBundleInfos(@Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
                                                                       @RequestParam int pageSize,
                                                                       @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
                                                                       @RequestParam int page,
                                                                       @Parameter(description = "Case-insensitive 'substring' filter based on app's name")
                                                                       @RequestParam(required = false) String textSearch,
                                                                       @Parameter(description = SORT_PROPERTY_DESCRIPTION)
                                                                       @RequestParam(required = false) String sortProperty,
                                                                       @Parameter(description = SORT_ORDER_DESCRIPTION)
                                                                       @RequestParam(required = false) String sortOrder) throws JnksIotException {
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        return mobileAppBundleService.findMobileAppBundleInfosByTenantId(getTenantId(), pageLink);
    }

    /**
     * 按 Id 读取 Bundle 详情。
     */
    @ApiOperation(value = "Get mobile app bundle info by id (getMobileAppBundleInfoById)", notes = SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @GetMapping(value = "/mobile/bundle/info/{id}")
    public MobileAppBundleInfo getMobileAppBundleInfoById(@PathVariable UUID id) throws JnksIotException {
        MobileAppBundleId mobileAppBundleId = new MobileAppBundleId(id);
        return checkEntityId(mobileAppBundleId, mobileAppBundleService::findMobileAppBundleInfoById, Operation.READ);
    }

    /**
     * 按 Id 删除 Bundle。引用不存在的 Id 会报错。
     */
    @ApiOperation(value = "Delete Mobile App Bundle by ID (deleteMobileAppBundle)",
            notes = "Deletes Mobile App Bundle by ID. Referencing non-existing mobile app bundle Id will cause an error." + SYSTEM_OR_TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @DeleteMapping(value = "/mobile/bundle/{id}")
    public void deleteMobileAppBundle(@PathVariable UUID id) throws Exception {
        MobileAppBundleId mobileAppBundleId = new MobileAppBundleId(id);
        MobileAppBundle mobileAppBundle = checkMobileAppBundleId(mobileAppBundleId, Operation.DELETE);
        jnksIotMobileAppBundleService.delete(mobileAppBundle, getCurrentUser());
    }

}
