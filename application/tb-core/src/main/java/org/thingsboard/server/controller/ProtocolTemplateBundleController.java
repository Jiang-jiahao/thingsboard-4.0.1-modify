/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.controller;

import io.swagger.v3.oas.annotations.Parameter;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateBundle;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.protocoltemplate.DefaultProtocolTemplateBundleService;
import org.thingsboard.server.service.protocoltemplate.ProtocolTemplateHexBuildRequest;
import org.thingsboard.server.service.protocoltemplate.ProtocolTemplateHexBuildResult;
import org.thingsboard.server.service.protocoltemplate.ProtocolTemplateHexBuildService;
import org.thingsboard.server.service.protocoltemplate.ProtocolTemplateHexParseRequest;
import org.thingsboard.server.service.protocoltemplate.ProtocolTemplateHexParseResult;
import org.thingsboard.server.service.protocoltemplate.ProtocolTemplateHexParseService;

import java.util.List;
import java.util.UUID;

/**
 * 协议模板包 REST 入口（自定义扩展）。
 * <p>
 * <b>职责：</b>管理当前租户的协议模板包（CRUD），以及与 TCP HEX 传输一致的上行 HEX 解析、
 * 下行 HEX 组帧。
 * <p>
 * <b>URL：</b>{@code /api/protocolTemplateBundle*}、{@code /api/protocolTemplateBundles}
 * <p>
 * <b>权限：</b>写/删仅 {@code TENANT_ADMIN}；列表与 HEX 解析/组帧 {@code TENANT_ADMIN} 或 {@code CUSTOMER_USER}。
 * 客户用户列表时只返回对其可见的包。
 * <p>
 * <b>下游：</b>{@link DefaultProtocolTemplateBundleService}、
 * {@link ProtocolTemplateHexParseService}、{@link ProtocolTemplateHexBuildService}
 */
@RestController
@TbCoreComponent
@RequestMapping("/api")
@RequiredArgsConstructor
public class ProtocolTemplateBundleController extends BaseController {

    private final DefaultProtocolTemplateBundleService protocolTemplateBundleService;
    private final ProtocolTemplateHexParseService protocolTemplateHexParseService;
    private final ProtocolTemplateHexBuildService protocolTemplateHexBuildService;

    /**
     * 列出当前租户可见的协议模板包。租户管理员看全部，客户用户按可见范围过滤。
     * <p>
     * 权限：{@code TENANT_ADMIN} 或 {@code CUSTOMER_USER}。下游 {@link DefaultProtocolTemplateBundleService#findAllForTenant}。
     */
    @ApiOperation(value = "List protocol template bundles for current tenant")
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN','CUSTOMER_USER')")
    @GetMapping("/protocolTemplateBundles")
    public List<ProtocolTemplateBundle> getProtocolTemplateBundles() throws ThingsboardException {
        TenantId tenantId = getTenantId();
        boolean tenantAdmin = SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream()
                .anyMatch(a -> "TENANT_ADMIN".equals(a.getAuthority()));
        return protocolTemplateBundleService.findAllForTenant(tenantId, tenantAdmin);
    }

    /**
     * 创建或更新协议模板包。参数非法时转为平台异常。
     * <p>
     * 权限：{@code TENANT_ADMIN}。下游 {@link DefaultProtocolTemplateBundleService#save}。
     */
    @ApiOperation(value = "Create or update protocol template bundle")
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @PostMapping("/protocolTemplateBundle")
    public ProtocolTemplateBundle saveProtocolTemplateBundle(
            @Parameter(description = "Protocol bundle JSON", required = true)
            @RequestBody ProtocolTemplateBundle bundle) throws ThingsboardException {
        try {
            return protocolTemplateBundleService.save(getTenantId(), bundle);
        } catch (IllegalArgumentException e) {
            throw handleException(e);
        }
    }

    /**
     * 按 id 删除协议模板包。
     * <p>
     * 权限：{@code TENANT_ADMIN}。下游 {@link DefaultProtocolTemplateBundleService#delete}。
     */
    @ApiOperation(value = "Delete protocol template bundle by id")
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @DeleteMapping("/protocolTemplateBundle/{id}")
    public void deleteProtocolTemplateBundle(
            @Parameter(description = "Bundle id (UUID)", required = true)
            @PathVariable("id") String strId) throws ThingsboardException {
        UUID id = UUID.fromString(strId);
        protocolTemplateBundleService.delete(getTenantId(), id);
    }

    /**
     * 用协议模板包解析 HEX 上行帧，规则与 TCP HEX 传输侧一致。
     * <p>
     * 权限：{@code TENANT_ADMIN} 或 {@code CUSTOMER_USER}。下游 {@link ProtocolTemplateHexParseService#parse}。
     */
    @ApiOperation(value = "Parse HEX uplink using a protocol template bundle (same as TCP HEX transport)")
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN','CUSTOMER_USER')")
    @PostMapping("/protocolTemplateBundle/parseHex")
    public ProtocolTemplateHexParseResult parseProtocolTemplateHex(
            @RequestBody ProtocolTemplateHexParseRequest request) throws ThingsboardException {
        checkNotNull(request);
        return protocolTemplateHexParseService.parse(getTenantId(), request.getBundleId(), request.getHex());
    }

    /**
     * 按协议模板包组下行 HEX 帧（DOWNLINK/BOTH 命令 + 合并字段）。
     * <p>
     * 权限：{@code TENANT_ADMIN} 或 {@code CUSTOMER_USER}。下游 {@link ProtocolTemplateHexBuildService#build}。
     */
    @ApiOperation(value = "Build HEX downlink frame from protocol template bundle (DOWNLINK/BOTH command + merged fields)")
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN','CUSTOMER_USER')")
    @PostMapping("/protocolTemplateBundle/buildHex")
    public ProtocolTemplateHexBuildResult buildProtocolTemplateHex(
            @RequestBody ProtocolTemplateHexBuildRequest request) throws ThingsboardException {
        checkNotNull(request);
        try {
            return protocolTemplateHexBuildService.build(getTenantId(), request);
        } catch (IllegalArgumentException e) {
            throw handleException(e);
        }
    }
}
