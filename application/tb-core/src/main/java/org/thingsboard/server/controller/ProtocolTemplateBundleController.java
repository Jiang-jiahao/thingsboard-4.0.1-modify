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

@RestController
@TbCoreComponent
@RequestMapping("/api")
@RequiredArgsConstructor
public class ProtocolTemplateBundleController extends BaseController {

    private final DefaultProtocolTemplateBundleService protocolTemplateBundleService;
    private final ProtocolTemplateHexParseService protocolTemplateHexParseService;
    private final ProtocolTemplateHexBuildService protocolTemplateHexBuildService;

    @ApiOperation(value = "List protocol template bundles for current tenant")
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN','CUSTOMER_USER')")
    @GetMapping("/protocolTemplateBundles")
    public List<ProtocolTemplateBundle> getProtocolTemplateBundles() throws ThingsboardException {
        TenantId tenantId = getTenantId();
        boolean tenantAdmin = SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream()
                .anyMatch(a -> "TENANT_ADMIN".equals(a.getAuthority()));
        return protocolTemplateBundleService.findAllForTenant(tenantId, tenantAdmin);
    }

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

    @ApiOperation(value = "Delete protocol template bundle by id")
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @DeleteMapping("/protocolTemplateBundle/{id}")
    public void deleteProtocolTemplateBundle(
            @Parameter(description = "Bundle id (UUID)", required = true)
            @PathVariable("id") String strId) throws ThingsboardException {
        UUID id = UUID.fromString(strId);
        protocolTemplateBundleService.delete(getTenantId(), id);
    }

    @ApiOperation(value = "Parse HEX uplink using a protocol template bundle (same as TCP HEX transport)")
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN','CUSTOMER_USER')")
    @PostMapping("/protocolTemplateBundle/parseHex")
    public ProtocolTemplateHexParseResult parseProtocolTemplateHex(
            @RequestBody ProtocolTemplateHexParseRequest request) throws ThingsboardException {
        checkNotNull(request);
        return protocolTemplateHexParseService.parse(getTenantId(), request.getBundleId(), request.getHex());
    }

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
