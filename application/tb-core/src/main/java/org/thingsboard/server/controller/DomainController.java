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
import org.thingsboard.server.common.data.domain.Domain;
import org.thingsboard.server.common.data.domain.DomainInfo;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.DomainId;
import org.thingsboard.server.common.data.id.OAuth2ClientId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.domain.TbDomainService;
import org.thingsboard.server.service.security.permission.Operation;
import org.thingsboard.server.service.security.permission.Resource;

import java.util.List;
import java.util.UUID;

import static org.thingsboard.server.controller.ControllerConstants.PAGE_NUMBER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_SIZE_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_ORDER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_PROPERTY_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SYSTEM_AUTHORITY_PARAGRAPH;
import static org.thingsboard.server.controller.ControllerConstants.UUID_WIKI_LINK;

/**
 * OAuth2 登录域名（Domain）REST 入口。
 * <p>
 * 系统管理员为平台配置对外域名，并把 OAuth2 客户端绑定到该域名。仅在
 * {@link TbCoreComponent} 中生效。域名名称在整个平台范围内唯一。
 * <p>
 * <b>URL 前缀：</b>{@code /api}。资源路径如 {@code /domain}、{@code /domain/infos}、
 * {@code /domain/{id}/oauth2Clients}。
 * <p>
 * <b>权限：</b>全部接口仅 SYS_ADMIN。
 * <p>
 * <b>下游：</b>写路径走 {@link TbDomainService}；分页/详情查询走基类 {@code domainService}。
 *
 * @see TbDomainService
 */
@RestController
@TbCoreComponent
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class DomainController extends BaseController {

    private final TbDomainService tbDomainService;

    /**
     * 创建或更新域名，并可同时绑定一组 OAuth2 客户端。
     * <p>
     * 无 {@code id} 为新建；指定已有 id 则更新。可选 {@code oauth2ClientIds} 逗号分隔。
     */
    @ApiOperation(value = "Save or Update Domain (saveDomain)",
            notes = "Create or update the Domain. When creating domain, platform generates Domain Id as " + UUID_WIKI_LINK +
                    "The newly created Domain Id will be present in the response. " +
                    "Specify existing Domain Id to update the domain. " +
                    "Referencing non-existing Domain Id will cause 'Not Found' error." +
                    "\n\nDomain name is unique for entire platform setup.\n\n" + SYSTEM_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN')")
    @PostMapping(value = "/domain")
    public Domain saveDomain(
            @Parameter(description = "A JSON value representing the Domain.", required = true)
            @RequestBody @Valid Domain domain,
            @Parameter(description = "A list of oauth2 client registration ids, separated by comma ','", array = @ArraySchema(schema = @Schema(type = "string")))
            @RequestParam(name = "oauth2ClientIds", required = false) UUID[] ids) throws Exception {
        domain.setTenantId(getTenantId());
        checkEntity(domain.getId(), domain, Resource.DOMAIN);
        return tbDomainService.save(domain, getOAuth2ClientIds(ids), getCurrentUser());
    }

    /**
     * 替换指定域名绑定的 OAuth2 客户端列表。
     */
    @ApiOperation(value = "Update oauth2 clients (updateOauth2Clients)",
            notes = "Update oauth2 clients for the specified domain. ")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN')")
    @PutMapping(value = "/domain/{id}/oauth2Clients")
    public void updateOauth2Clients(@PathVariable UUID id,
                                    @RequestBody UUID[] clientIds) throws ThingsboardException {
        DomainId domainId = new DomainId(id);
        Domain domain = checkDomainId(domainId, Operation.WRITE);
        List<OAuth2ClientId> oAuth2ClientIds = getOAuth2ClientIds(clientIds);
        tbDomainService.updateOauth2Clients(domain, oAuth2ClientIds, getCurrentUser());
    }

    /**
     * 分页列出当前租户（系统租户）下的域名信息（含关联 OAuth2 客户端摘要）。
     */
    @ApiOperation(value = "Get Domain infos (getTenantDomainInfos)", notes = SYSTEM_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN')")
    @GetMapping(value = "/domain/infos")
    public PageData<DomainInfo> getTenantDomainInfos(@Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
                                                     @RequestParam int pageSize,
                                                     @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
                                                     @RequestParam int page,
                                                     @Parameter(description = "Case-insensitive 'substring' filter based on domain's name")
                                                     @RequestParam(required = false) String textSearch,
                                                     @Parameter(description = SORT_PROPERTY_DESCRIPTION)
                                                     @RequestParam(required = false) String sortProperty,
                                                     @Parameter(description = SORT_ORDER_DESCRIPTION)
                                                     @RequestParam(required = false) String sortOrder) throws ThingsboardException {
        accessControlService.checkPermission(getCurrentUser(), Resource.DOMAIN, Operation.READ);
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        return domainService.findDomainInfosByTenantId(getTenantId(), pageLink);
    }

    /**
     * 按 Id 读取域名详情（含关联 OAuth2 客户端）。
     */
    @ApiOperation(value = "Get Domain info by Id (getDomainInfoById)", notes = SYSTEM_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN')")
    @GetMapping(value = "/domain/info/{id}")
    public DomainInfo getDomainInfoById(@PathVariable UUID id) throws ThingsboardException {
        DomainId domainId = new DomainId(id);
        return checkEntityId(domainId, domainService::findDomainInfoById, Operation.READ);
    }

    /**
     * 按 Id 删除域名。引用不存在的 Id 会报错。
     */
    @ApiOperation(value = "Delete Domain by ID (deleteDomain)",
            notes = "Deletes Domain by ID. Referencing non-existing domain Id will cause an error." + SYSTEM_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('SYS_ADMIN')")
    @DeleteMapping(value = "/domain/{id}")
    public void deleteDomain(@PathVariable UUID id) throws Exception {
        DomainId domainId = new DomainId(id);
        Domain domain = checkDomainId(domainId, Operation.DELETE);
        tbDomainService.delete(domain, getCurrentUser());
    }

}
