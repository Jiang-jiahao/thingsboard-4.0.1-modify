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
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateBundle;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateCommandDefinition;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateDefinition;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateTransportTcpDataConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.dao.device.DeviceProfileService;
import org.thingsboard.server.dao.model.sql.ProtocolTemplateBundleEntity;
import org.thingsboard.server.dao.sql.protocoltemplate.ProtocolTemplateBundleRepository;
import org.thingsboard.server.dao.tenant.TenantService;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
    private static final TypeReference<List<ProtocolTemplateDefinition>> TEMPLATE_LIST_TYPE =
            new TypeReference<>() {};
    private static final TypeReference<List<ProtocolTemplateCommandDefinition>> COMMAND_LIST_TYPE =
            new TypeReference<>() {};

    private final ProtocolTemplateBundleRepository bundleRepository;
    private final TenantService tenantService;
    private final DeviceProfileService deviceProfileService;

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
        String desc = bundle.getDescription() != null ? bundle.getDescription().trim() : "";
        entity.setDescription(desc.isEmpty() ? null : desc);
        entity.setBundleData(toBundleDataJson(bundle));

        ProtocolTemplateBundleEntity saved = bundleRepository.save(entity);
        ProtocolTemplateBundle savedDto = toDto(saved);
        refreshReferencedDeviceProfiles(tenantId, savedDto);
        return savedDto;
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
                String md = b.getDescription() != null ? b.getDescription().trim() : "";
                entity.setDescription(md.isEmpty() ? null : md);
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
        if (bundle.getDescription() != null) {
            String d = bundle.getDescription().trim();
            if (d.length() > 512) {
                throw new IllegalArgumentException("Protocol template bundle description must be at most 512 characters");
            }
        }
    }

    private static UUID parseOrCreateId(String idStr) {
        if (idStr == null || idStr.isBlank()) {
            return UUID.randomUUID();
        }
        return UUID.fromString(idStr.trim());
    }

    private void refreshReferencedDeviceProfiles(TenantId tenantId, ProtocolTemplateBundle savedBundle) {
        if (savedBundle.getId() == null || savedBundle.getId().isBlank()) {
            return;
        }
        PageLink pageLink = new PageLink(100);
        PageData<DeviceProfile> pageData;
        int refreshed = 0;
        do {
            pageData = deviceProfileService.findDeviceProfiles(tenantId, pageLink);
            for (DeviceProfile profile : pageData.getData()) {
                if (updateProtocolTemplateSnapshot(profile, savedBundle)) {
                    deviceProfileService.saveDeviceProfile(profile, true, true);
                    refreshed++;
                }
            }
            pageLink = pageLink.nextPageLink();
        } while (pageData.hasNext());
        if (refreshed > 0) {
            log.info("Refreshed {} device profiles for protocol template bundle {}", refreshed, savedBundle.getId());
        }
    }

    private static boolean updateProtocolTemplateSnapshot(DeviceProfile profile, ProtocolTemplateBundle savedBundle) {
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return false;
        }
        if (!(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration tcpTransportCfg)) {
            return false;
        }
        if (!(tcpTransportCfg.getTransportTcpDataTypeConfiguration() instanceof ProtocolTemplateTransportTcpDataConfiguration ptCfg)) {
            return false;
        }
        if (!Objects.equals(savedBundle.getId(), ptCfg.getProtocolTemplateBundleId())) {
            return false;
        }

        List<ProtocolTemplateDefinition> newTemplates = savedBundle.getProtocolTemplates() != null
                ? JacksonUtil.convertValue(savedBundle.getProtocolTemplates(), TEMPLATE_LIST_TYPE) : new ArrayList<>();
        List<ProtocolTemplateCommandDefinition> newCommands = savedBundle.getProtocolCommands() != null
                ? JacksonUtil.convertValue(savedBundle.getProtocolCommands(), COMMAND_LIST_TYPE) : new ArrayList<>();
        if (Objects.equals(ptCfg.getProtocolTemplates(), newTemplates)
                && Objects.equals(ptCfg.getProtocolCommands(), newCommands)) {
            return false;
        }
        ptCfg.setProtocolTemplates(newTemplates);
        ptCfg.setProtocolCommands(newCommands);
        profile.setProfileData(profile.getProfileData());
        return true;
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
        if (e.getDescription() != null) {
            String col = e.getDescription().trim();
            if (!col.isEmpty()) {
                b.setDescription(col);
            }
        }
        JsonNode data = e.getBundleData();
        if (data != null && data.isObject()) {
            b.setProtocolTemplates(readBundleList(
                    firstNonNull(data, "protocolTemplates", "monitoringTemplates"),
                    new TypeReference<List<ProtocolTemplateDefinition>>() {}));
            b.setProtocolCommands(readBundleList(
                    firstNonNull(data, "protocolCommands", "monitoringCommands"),
                    new TypeReference<List<ProtocolTemplateCommandDefinition>>() {}));
            if (b.getDescription() == null && data.has("description") && !data.get("description").isNull()) {
                JsonNode dn = data.get("description");
                if (dn.isTextual()) {
                    String d = dn.asText().trim();
                    if (!d.isEmpty()) {
                        b.setDescription(d);
                    }
                }
            }
        } else if (data != null && data.isArray()) {
            // 兼容误将「仅帧模板数组」写入 bundle_data 根的情况
            b.setProtocolTemplates(readBundleList(data, new TypeReference<List<ProtocolTemplateDefinition>>() {}));
            b.setProtocolCommands(new ArrayList<>());
        } else {
            b.setProtocolTemplates(new ArrayList<>());
            b.setProtocolCommands(new ArrayList<>());
        }
        return b;
    }

    private static JsonNode firstNonNull(JsonNode obj, String a, String b) {
        if (obj != null && obj.hasNonNull(a)) {
            return obj.get(a);
        }
        if (obj != null && obj.hasNonNull(b)) {
            return obj.get(b);
        }
        return null;
    }

    /**
     * bundle_data 中列表可能为 JSON 数组，也可能被错误存成字符串；反序列化时忽略未知字段（如历史版本中的 validateTotalLengthU32Le）。
     */
    private static <T> List<T> readBundleList(JsonNode raw, TypeReference<List<T>> typeRef) {
        JsonNode node = unwrapJsonStringToNode(raw);
        if (node == null || !node.isArray()) {
            return new ArrayList<>();
        }
        return JacksonUtil.IGNORE_UNKNOWN_PROPERTIES_JSON_MAPPER.convertValue(node, typeRef);
    }

    private static JsonNode unwrapJsonStringToNode(JsonNode raw) {
        if (raw == null || raw.isNull()) {
            return null;
        }
        if (raw.isTextual()) {
            String t = raw.asText();
            if (t == null || t.isBlank()) {
                return JacksonUtil.newArrayNode();
            }
            return JacksonUtil.toJsonNode(t.trim());
        }
        return raw;
    }
}
