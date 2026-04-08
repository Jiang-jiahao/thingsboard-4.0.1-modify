/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.service.protocoltemplate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateBundle;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateCommandDefinition;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateDefinition;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.dao.model.sql.ProtocolTemplateBundleEntity;
import org.thingsboard.server.dao.sql.protocoltemplate.ProtocolTemplateBundleRepository;
import org.thingsboard.server.dao.tenant.TenantService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class DefaultProtocolTemplateBundleService {

    /** Tenant.additionalInfo 中协议模板包列表键名 */
    public static final String PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY = "protocolTemplateBundles";

    /** 旧版 tenant.additionalInfo 键名（迁移时读取） */
    private static final String LEGACY_PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY = "monitoringProtocolBundles";

    private final ProtocolTemplateBundleRepository bundleRepository;
    private final TenantService tenantService;

    @Transactional(readOnly = true)
    public Optional<ProtocolTemplateBundle> findById(TenantId tenantId, UUID id) {
        return bundleRepository.findByTenantIdAndId(tenantId.getId(), id).map(this::toDto);
    }

    @Transactional
    public List<ProtocolTemplateBundle> findAllForTenant(TenantId tenantId, boolean migrateFromAdditionalInfo) {
        if (migrateFromAdditionalInfo) {
            migrateFromTenantAdditionalInfoIfNeeded(tenantId);
        }
        return bundleRepository.findAllByTenantIdOrderByCreatedTimeAsc(tenantId.getId()).stream()
                .map(this::toDto)
                .collect(Collectors.toList());
    }

    @Transactional
    public ProtocolTemplateBundle save(TenantId tenantId, ProtocolTemplateBundle bundle) {
        validateBundleContent(bundle);
        migrateFromTenantAdditionalInfoIfNeeded(tenantId);

        UUID id = parseOrCreateId(bundle.getId());
        long now = System.currentTimeMillis();

        ProtocolTemplateBundleEntity entity = bundleRepository.findByTenantIdAndId(tenantId.getId(), id).orElse(null);
        if (entity == null) {
            entity = new ProtocolTemplateBundleEntity();
            entity.setId(id);
            entity.setCreatedTime(now);
            entity.setTenantId(tenantId.getId());
        } else {
            entity.setCreatedTime(entity.getCreatedTime());
        }
        entity.setName(bundle.getName());
        entity.setBundleData(toBundleDataJson(bundle));

        ProtocolTemplateBundleEntity saved = bundleRepository.save(entity);
        return toDto(saved);
    }

    @Transactional
    public void delete(TenantId tenantId, UUID id) {
        bundleRepository.deleteByTenantIdAndId(tenantId.getId(), id);
    }

    private void migrateFromTenantAdditionalInfoIfNeeded(TenantId tenantId) {
        if (bundleRepository.countByTenantId(tenantId.getId()) > 0) {
            return;
        }
        Tenant tenant = tenantService.findTenantById(tenantId);
        if (tenant == null || tenant.getAdditionalInfo() == null || !tenant.getAdditionalInfo().isObject()) {
            return;
        }
        ObjectNode additionalInfo = (ObjectNode) tenant.getAdditionalInfo();
        JsonNode raw = null;
        if (additionalInfo.has(PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY)) {
            raw = additionalInfo.get(PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY);
        } else if (additionalInfo.has(LEGACY_PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY)) {
            raw = additionalInfo.get(LEGACY_PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY);
        }
        if (raw == null) {
            return;
        }
        if (!raw.isArray() || raw.isEmpty()) {
            additionalInfo.remove(PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY);
            additionalInfo.remove(LEGACY_PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY);
            tenant.setAdditionalInfo(additionalInfo);
            tenantService.saveTenant(tenant);
            return;
        }
        for (JsonNode item : raw) {
            try {
                ProtocolTemplateBundle b = JacksonUtil.treeToValue(item, ProtocolTemplateBundle.class);
                if (b == null || b.getId() == null || b.getId().isBlank()) {
                    continue;
                }
                validateBundleContent(b);
                UUID bid = UUID.fromString(b.getId());
                ProtocolTemplateBundleEntity entity = new ProtocolTemplateBundleEntity();
                entity.setId(bid);
                entity.setCreatedTime(System.currentTimeMillis());
                entity.setTenantId(tenantId.getId());
                entity.setName(b.getName());
                entity.setBundleData(toBundleDataJson(b));
                bundleRepository.save(entity);
            } catch (Exception e) {
                log.warn("Skipping invalid protocol template bundle during migration: {}", e.getMessage());
            }
        }
        additionalInfo.remove(PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY);
        additionalInfo.remove(LEGACY_PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY);
        tenant.setAdditionalInfo(additionalInfo);
        tenantService.saveTenant(tenant);
        log.info("Migrated protocol template bundles from tenant additionalInfo to database for tenant {}", tenantId);
    }

    private static void validateBundleContent(ProtocolTemplateBundle bundle) {
        List<ProtocolTemplateDefinition> t = bundle.getProtocolTemplates() != null ? bundle.getProtocolTemplates() : List.of();
        List<ProtocolTemplateCommandDefinition> c = bundle.getProtocolCommands() != null ? bundle.getProtocolCommands() : List.of();
        if (t.isEmpty() && c.isEmpty()) {
            return;
        }
        if (t.isEmpty() || c.isEmpty()) {
            throw new IllegalArgumentException("Protocol template bundle must have both templates and commands, or neither.");
        }
    }

    private static UUID parseOrCreateId(String idStr) {
        if (idStr == null || idStr.isBlank()) {
            return UUID.randomUUID();
        }
        return UUID.fromString(idStr.trim());
    }

    private JsonNode toBundleDataJson(ProtocolTemplateBundle bundle) {
        ObjectNode root = JacksonUtil.newObjectNode();
        root.set("protocolTemplates", JacksonUtil.valueToTree(
                bundle.getProtocolTemplates() != null ? bundle.getProtocolTemplates() : List.of()));
        root.set("protocolCommands", JacksonUtil.valueToTree(
                bundle.getProtocolCommands() != null ? bundle.getProtocolCommands() : List.of()));
        return root;
    }

    private ProtocolTemplateBundle toDto(ProtocolTemplateBundleEntity e) {
        ProtocolTemplateBundle b = new ProtocolTemplateBundle();
        b.setId(e.getId().toString());
        b.setCreatedTime(e.getCreatedTime());
        b.setName(e.getName());
        JsonNode data = e.getBundleData();
        if (data != null && data.isObject()) {
            if (data.hasNonNull("protocolTemplates")) {
                b.setProtocolTemplates(JacksonUtil.convertValue(data.get("protocolTemplates"),
                        new TypeReference<List<ProtocolTemplateDefinition>>() {}));
            } else if (data.hasNonNull("monitoringTemplates")) {
                b.setProtocolTemplates(JacksonUtil.convertValue(data.get("monitoringTemplates"),
                        new TypeReference<List<ProtocolTemplateDefinition>>() {}));
            } else {
                b.setProtocolTemplates(new ArrayList<>());
            }
            if (data.hasNonNull("protocolCommands")) {
                b.setProtocolCommands(JacksonUtil.convertValue(data.get("protocolCommands"),
                        new TypeReference<List<ProtocolTemplateCommandDefinition>>() {}));
            } else if (data.hasNonNull("monitoringCommands")) {
                b.setProtocolCommands(JacksonUtil.convertValue(data.get("monitoringCommands"),
                        new TypeReference<List<ProtocolTemplateCommandDefinition>>() {}));
            } else {
                b.setProtocolCommands(new ArrayList<>());
            }
        } else {
            b.setProtocolTemplates(new ArrayList<>());
            b.setProtocolCommands(new ArrayList<>());
        }
        return b;
    }
}
