package com.jnks.iot.server.dao.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.event.TransactionalEventListener;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.cache.resourceInfo.ResourceInfoCacheKey;
import com.jnks.iot.server.cache.resourceInfo.ResourceInfoEvictEvent;
import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.ResourceExportData;
import com.jnks.iot.server.common.data.ResourceSubType;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.JnksIotResource;
import com.jnks.iot.server.common.data.JnksIotResourceDeleteResult;
import com.jnks.iot.server.common.data.JnksIotResourceInfo;
import com.jnks.iot.server.common.data.JnksIotResourceInfoFilter;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.HasId;
import com.jnks.iot.server.common.data.id.JnksIotResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;
import com.jnks.iot.server.dao.ResourceContainerDao;
import com.jnks.iot.server.dao.dashboard.DashboardInfoDao;
import com.jnks.iot.server.dao.entity.AbstractCachedEntityService;
import com.jnks.iot.server.dao.eventsourcing.DeleteEntityEvent;
import com.jnks.iot.server.dao.eventsourcing.SaveEntityEvent;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.PaginatedRemover;
import com.jnks.iot.server.dao.service.Validator;
import com.jnks.iot.server.dao.service.validator.ResourceDataValidator;
import com.jnks.iot.server.dao.widget.WidgetTypeDao;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import static com.jnks.iot.server.common.data.StringUtils.isNotEmpty;
import static com.jnks.iot.server.dao.device.DeviceServiceImpl.INCORRECT_TENANT_ID;
import static com.jnks.iot.server.dao.service.Validator.validateId;

@Service("JnksIotResourceDaoService")
@Slf4j
@RequiredArgsConstructor
@Primary
public class BaseResourceService extends AbstractCachedEntityService<ResourceInfoCacheKey, JnksIotResourceInfo, ResourceInfoEvictEvent> implements ResourceService {

    public static final String INCORRECT_RESOURCE_ID = "Incorrect resourceId ";
    protected final JnksIotResourceDao resourceDao;
    protected final JnksIotResourceInfoDao resourceInfoDao;
    protected final ResourceDataValidator resourceValidator;
    protected final WidgetTypeDao widgetTypeDao;
    protected final DashboardInfoDao dashboardInfoDao;
    private final Map<EntityType, ResourceContainerDao<?>> resourceContainerDaoMap = new HashMap<>();
    protected static final int MAX_ENTITIES_TO_FIND = 10;

    @PostConstruct
    public void init() {
        resourceContainerDaoMap.put(EntityType.WIDGET_TYPE, widgetTypeDao);
        resourceContainerDaoMap.put(EntityType.DASHBOARD, dashboardInfoDao);
    }

    @Autowired @Lazy
    private ImageService imageService;

    private static final Map<String, String> DASHBOARD_RESOURCES_MAPPING = Map.of(
            "widgets.*.config.actions.*.*.customResources.*.url", ""
    );
    private static final Map<String, String> WIDGET_RESOURCES_MAPPING = Map.of(
            "resources.*.url", ""
    );
    private static final Map<String, String> WIDGET_DEFAULT_CONFIG_RESOURCES_MAPPING = Map.of(
            "actions.*.*.customResources.*.url", ""
    );

    @Override
    public JnksIotResource saveResource(JnksIotResource resource, boolean doValidate) {
        log.trace("Executing saveResource [{}]", resource);
        if (resource.getTenantId() == null) {
            resource.setTenantId(TenantId.SYS_TENANT_ID);
        }
        if (resource.getId() == null) {
            resource.setResourceKey(getUniqueKey(resource.getTenantId(), resource.getResourceType(), StringUtils.defaultIfEmpty(resource.getResourceKey(), resource.getFileName())));
        }
        if (doValidate) {
            resourceValidator.validate(resource, JnksIotResourceInfo::getTenantId);
        }
        if (resource.getData() != null) {
            resource.setEtag(calculateEtag(resource.getData()));
        }
        return doSaveResource(resource);
    }

    @Override
    public JnksIotResource saveResource(JnksIotResource resource) {
        return saveResource(resource, true);
    }

    protected JnksIotResource doSaveResource(JnksIotResource resource) {
        TenantId tenantId = resource.getTenantId();
        try {
            JnksIotResource saved;
            if (resource.getData() != null) {
                saved = resourceDao.save(tenantId, resource);
            } else {
                JnksIotResourceInfo resourceInfo = saveResourceInfo(resource);
                saved = new JnksIotResource(resourceInfo);
            }
            publishEvictEvent(new ResourceInfoEvictEvent(tenantId, resource.getId()));
            eventPublisher.publishEvent(SaveEntityEvent.builder().tenantId(saved.getTenantId()).entityId(saved.getId())
                    .entity(saved).created(resource.getId() == null).build());
            return saved;
        } catch (Exception t) {
            publishEvictEvent(new ResourceInfoEvictEvent(tenantId, resource.getId()));
            ConstraintViolationException e = extractConstraintViolationException(t).orElse(null);
            if (e != null && e.getConstraintName() != null && e.getConstraintName().equalsIgnoreCase("resource_unq_key")) {
                throw new DataValidationException("Resource with such key already exists!");
            } else {
                throw t;
            }
        }
    }

    private JnksIotResourceInfo saveResourceInfo(JnksIotResource resource) {
        return resourceInfoDao.save(resource.getTenantId(), new JnksIotResourceInfo(resource));
    }

    protected String getUniqueKey(TenantId tenantId, ResourceType resourceType, String filename) {
        if (!resourceInfoDao.existsByTenantIdAndResourceTypeAndResourceKey(tenantId, resourceType, filename)) {
            return filename;
        }

        String basename = StringUtils.substringBeforeLast(filename, ".");
        String extension = StringUtils.substringAfterLast(filename, ".");

        Set<String> existing = resourceInfoDao.findKeysByTenantIdAndResourceTypeAndResourceKeyPrefix(
                tenantId, resourceType, basename
        );
        String resourceKey = filename;
        int idx = 1;
        while (existing.contains(resourceKey)) {
            resourceKey = basename + "_(" + idx + ")" + (!extension.isEmpty() ? "." + extension : "");
            idx++;
        }
        log.debug("[{}] Generated unique key {} for {} {}", tenantId, resourceKey, resourceType, filename);
        return resourceKey;
    }

    @Override
    public JnksIotResource findResourceByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceKey) {
        log.trace("Executing findResourceByTenantIdAndKey [{}] [{}] [{}]", tenantId, resourceType, resourceKey);
        return resourceDao.findResourceByTenantIdAndKey(tenantId, resourceType, resourceKey);
    }

    @Override
    public JnksIotResource findResourceById(TenantId tenantId, JnksIotResourceId resourceId) {
        log.trace("Executing findResourceById [{}] [{}]", tenantId, resourceId);
        Validator.validateId(resourceId, id -> INCORRECT_RESOURCE_ID + id);
        return resourceDao.findById(tenantId, resourceId.getId());
    }

    @Override
    public byte[] getResourceData(TenantId tenantId, JnksIotResourceId resourceId) {
        log.trace("Executing getResourceData [{}] [{}]", tenantId, resourceId);
        return resourceDao.getResourceData(tenantId, resourceId);
    }

    @Override
    public ResourceExportData exportResource(JnksIotResourceInfo resourceInfo) {
        byte[] data = getResourceData(resourceInfo.getTenantId(), resourceInfo.getId());
        return ResourceExportData.builder()
                .link(resourceInfo.getLink())
                .mediaType(resourceInfo.getResourceType().getMediaType())
                .fileName(resourceInfo.getFileName())
                .title(resourceInfo.getTitle())
                .type(resourceInfo.getResourceType())
                .subType(resourceInfo.getResourceSubType())
                .resourceKey(resourceInfo.getResourceKey())
                .data(Base64.getEncoder().encodeToString(data))
                .build();
    }

    @Override
    public List<ResourceExportData> exportResources(TenantId tenantId, Collection<JnksIotResourceInfo> resources) {
        return resources.stream()
                .sorted(Comparator.comparing(JnksIotResourceInfo::getResourceType).thenComparing(JnksIotResourceInfo::getResourceKey))
                .map(resourceInfo -> {
                    if (resourceInfo.getResourceType() == ResourceType.IMAGE) {
                        ResourceExportData imageExportData = imageService.exportImage(resourceInfo);
                        imageExportData.setResourceKey(null); // so that the image is not updated by resource key on import
                        return imageExportData;
                    } else {
                        return exportResource(resourceInfo);
                    }
                })
                .toList();
    }

    @Override
    public void importResources(TenantId tenantId, List<ResourceExportData> resources) {
        for (ResourceExportData resourceData : resources) {
            if (resourceData.getNewLink() != null) {
                continue; // already imported
            }

            JnksIotResource resource;
            if (resourceData.getType() == ResourceType.IMAGE) {
                resource = imageService.toImage(tenantId, resourceData, true);
                if (resource.getData() != null) {
                    imageService.saveImage(resource);
                }
            } else {
                resource = toResource(tenantId, resourceData);
                if (resource.getData() != null) {
                    saveResource(resource);
                }
            }
            resourceData.setNewLink(resource.getLink());
        }
    }

    @Override
    public JnksIotResource toResource(TenantId tenantId, ResourceExportData exportData) {
        if (exportData.getType() == ResourceType.IMAGE || exportData.getSubType() == ResourceSubType.IMAGE
                || exportData.getSubType() == ResourceSubType.SCADA_SYMBOL) {
            throw new IllegalArgumentException("Image import not supported");
        }

        byte[] data = Base64.getDecoder().decode(exportData.getData());
        String etag = calculateEtag(data);

        JnksIotResourceInfo existingResource;
        boolean update = false;
        if (!tenantId.isSysTenantId()) {
            existingResource = findSystemOrTenantResourceByEtag(tenantId, exportData.getType(), etag);
        } else {
            existingResource = findResourceInfoByTenantIdAndKey(tenantId, exportData.getType(), exportData.getResourceKey());
            update = true; // we overwrite system resource instead of creating new
        }
        if (existingResource != null) {
            JnksIotResource resource = new JnksIotResource(existingResource);
            if (update && !etag.equals(resource.getEtag())) {
                resource.setData(data);
                resource.setTitle(exportData.getTitle());
                log.debug("[{}] Updating existing resource {}", tenantId, existingResource.getLink());
            } else {
                log.debug("[{}] Using existing resource {}", tenantId, existingResource.getLink());
            }
            return resource;
        }

        JnksIotResource resource = new JnksIotResource();
        resource.setTenantId(tenantId);
        resource.setFileName(exportData.getFileName());
        if (isNotEmpty(exportData.getTitle())) {
            resource.setTitle(exportData.getTitle());
        } else {
            resource.setTitle(exportData.getFileName());
        }
        resource.setResourceSubType(exportData.getSubType());
        resource.setResourceType(exportData.getType());
        resource.setResourceKey(exportData.getResourceKey());
        resource.setData(data);
        log.debug("[{}] Creating resource {}", tenantId, resource.getResourceKey());
        return resource;
    }

    @Override
    public JnksIotResourceInfo findResourceInfoById(TenantId tenantId, JnksIotResourceId resourceId) {
        log.trace("Executing findResourceInfoById [{}] [{}]", tenantId, resourceId);
        Validator.validateId(resourceId, id -> INCORRECT_RESOURCE_ID + id);

        return cache.getAndPutInTransaction(new ResourceInfoCacheKey(tenantId, resourceId),
                () -> resourceInfoDao.findById(tenantId, resourceId.getId()), true);
    }

    @Override
    public JnksIotResourceInfo findResourceInfoByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceKey) {
        log.trace("Executing findResourceInfoByTenantIdAndKey [{}] [{}] [{}]", tenantId, resourceType, resourceKey);
        return resourceInfoDao.findByTenantIdAndKey(tenantId, resourceType, resourceKey);
    }

    @Override
    public ListenableFuture<JnksIotResourceInfo> findResourceInfoByIdAsync(TenantId tenantId, JnksIotResourceId resourceId) {
        log.trace("Executing findResourceInfoById [{}] [{}]", tenantId, resourceId);
        Validator.validateId(resourceId, id -> INCORRECT_RESOURCE_ID + id);
        return resourceInfoDao.findByIdAsync(tenantId, resourceId.getId());
    }

    @Override
    public JnksIotResourceDeleteResult deleteResource(TenantId tenantId, JnksIotResourceId resourceId, boolean force) {
        log.trace("Executing deleteResource [{}] [{}]", tenantId, resourceId);
        Validator.validateId(resourceId, id -> INCORRECT_RESOURCE_ID + id);
        JnksIotResourceInfo resource = findResourceInfoById(tenantId, resourceId);
        boolean success = true;
        var result = JnksIotResourceDeleteResult.builder();

        if (resource == null) {
            if (!force) {
                success = false;
            }
            return result.success(success).build();
        }

        if (!force) {
            if (resource.getResourceType() == ResourceType.JS_MODULE) {
                var link = resource.getLink();
                Map<String, List<? extends HasId<?>>> affectedEntities = new HashMap<>();

                resourceContainerDaoMap.forEach((entityType, resourceContainerDao) -> {
                    var entities = tenantId.isSysTenantId() ? resourceContainerDao.findByResourceLink(link, MAX_ENTITIES_TO_FIND) :
                            resourceContainerDao.findByTenantIdAndResourceLink(tenantId, link, MAX_ENTITIES_TO_FIND);
                    if (!entities.isEmpty()) {
                        affectedEntities.put(entityType.name(), entities);
                    }
                });

                if (!affectedEntities.isEmpty()) {
                    success = false;
                    result.references(affectedEntities);
                }
            }
        }
        if (success) {
            resourceDao.removeById(tenantId, resourceId.getId());
            eventPublisher.publishEvent(DeleteEntityEvent.builder().tenantId(tenantId).entity(resource).entityId(resourceId).build());
        }

        return result.success(success).build();
    }

    @Override
    public void deleteEntity(TenantId tenantId, EntityId id, boolean force) {
        deleteResource(tenantId, (JnksIotResourceId) id, force);
    }

    @Override
    public PageData<JnksIotResourceInfo> findAllTenantResourcesByTenantId(JnksIotResourceInfoFilter filter, PageLink pageLink) {
        TenantId tenantId = filter.getTenantId();
        log.trace("Executing findAllTenantResourcesByTenantId [{}]", tenantId);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        return resourceInfoDao.findAllTenantResourcesByTenantId(filter, pageLink);
    }

    @Override
    public PageData<JnksIotResourceInfo> findTenantResourcesByTenantId(JnksIotResourceInfoFilter filter, PageLink pageLink) {
        TenantId tenantId = filter.getTenantId();
        log.trace("Executing findTenantResourcesByTenantId [{}]", tenantId);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        return resourceInfoDao.findTenantResourcesByTenantId(filter, pageLink);
    }

    @Override
    public List<JnksIotResource> findTenantResourcesByResourceTypeAndObjectIds(TenantId tenantId, ResourceType resourceType, String[] objectIds) {
        log.trace("Executing findTenantResourcesByResourceTypeAndObjectIds [{}][{}][{}]", tenantId, resourceType, objectIds);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        return resourceDao.findResourcesByTenantIdAndResourceType(tenantId, resourceType, null, objectIds, null);
    }

    @Override
    public PageData<JnksIotResource> findAllTenantResources(TenantId tenantId, PageLink pageLink) {
        log.trace("Executing findAllTenantResources [{}][{}]", tenantId, pageLink);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        return resourceDao.findAllByTenantId(tenantId, pageLink);
    }

    @Override
    public PageData<JnksIotResource> findTenantResourcesByResourceTypeAndPageLink(TenantId tenantId, ResourceType resourceType, PageLink pageLink) {
        log.trace("Executing findTenantResourcesByResourceTypeAndPageLink [{}][{}][{}]", tenantId, resourceType, pageLink);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        return resourceDao.findResourcesByTenantIdAndResourceType(tenantId, resourceType, null, pageLink);
    }

    @Override
    public void deleteResourcesByTenantId(TenantId tenantId) {
        log.trace("Executing deleteResourcesByTenantId, tenantId [{}]", tenantId);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        tenantResourcesRemover.removeEntities(tenantId, tenantId);
    }

    @Override
    public void deleteByTenantId(TenantId tenantId) {
        deleteResourcesByTenantId(tenantId);
    }

    @Override
    public Optional<HasId<?>> findEntity(TenantId tenantId, EntityId entityId) {
        return Optional.ofNullable(findResourceInfoById(tenantId, new JnksIotResourceId(entityId.getId())));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.JNKS_IOT_RESOURCE;
    }

    @Override
    public long sumDataSizeByTenantId(TenantId tenantId) {
        return resourceDao.sumDataSizeByTenantId(tenantId);
    }

    @Override
    public boolean updateResourcesUsage(TenantId tenantId, Dashboard dashboard) {
        if (dashboard.getConfiguration() == null) {
            return false;
        }
        Map<String, String> links = getResourcesLinks(dashboard.getResources());
        return updateResourcesUsage(tenantId, List.of(dashboard.getConfiguration()), List.of(DASHBOARD_RESOURCES_MAPPING), links);
    }

    @Override
    public boolean updateResourcesUsage(TenantId tenantId, WidgetTypeDetails widgetTypeDetails) {
        Map<String, String> links = getResourcesLinks(widgetTypeDetails.getResources());
        List<JsonNode> jsonNodes = new ArrayList<>(2);
        List<Map<String, String>> mappings = new ArrayList<>(2);

        if (widgetTypeDetails.getDescriptor() != null) {
            jsonNodes.add(widgetTypeDetails.getDescriptor());
            mappings.add(WIDGET_RESOURCES_MAPPING);
        }

        JsonNode defaultConfig = widgetTypeDetails.getDefaultConfig();
        if (defaultConfig != null) {
            jsonNodes.add(defaultConfig);
            mappings.add(WIDGET_DEFAULT_CONFIG_RESOURCES_MAPPING);
        }

        boolean updated = updateResourcesUsage(tenantId, jsonNodes, mappings, links);
        if (defaultConfig != null) {
            widgetTypeDetails.setDefaultConfig(defaultConfig);
        }
        return updated;
    }

    protected Map<String, String> getResourcesLinks(List<ResourceExportData> resources) {
        Map<String, String> links;
        if (CollectionUtils.isNotEmpty(resources)) {
            links = new HashMap<>();
            resources.forEach(resource -> {
                if (resource.getNewLink() != null) {
                    links.put(resource.getLink(), resource.getNewLink());
                }
            });
        } else {
            links = Collections.emptyMap();
        }
        return links;
    }

    private boolean updateResourcesUsage(TenantId tenantId, List<JsonNode> jsonNodes, List<Map<String, String>> mappings, Map<String, String> links) {
        log.trace("[{}] updateResourcesUsage (new links: {}) for {}", tenantId, links, jsonNodes);
        return processResources(jsonNodes, mappings, value -> {
            String link = getResourceLink(value);
            if (link != null) {
                String newLink = links.get(link);
                if (newLink == null || newLink.equals(link)) {
                    return value; // leaving link as is
                } else {
                    return DataConstants.JNKS_IOT_RESOURCE_PREFIX + newLink;
                }
            } else { // probably importing an old dashboard json where resources are referenced by ids
                JnksIotResourceId resourceId;
                try {
                    resourceId = new JnksIotResourceId(UUID.fromString(value));
                } catch (IllegalArgumentException e) {
                    return value;
                }
                JnksIotResourceInfo resourceInfo = findResourceInfoById(tenantId, resourceId);
                if (resourceInfo != null) {
                    return DataConstants.JNKS_IOT_RESOURCE_PREFIX + resourceInfo.getLink();
                } else {
                    log.warn("[{}] Couldn't find resource referenced as '{}'", tenantId, value);
                    return "";
                }
            }
        });
    }

    @Override
    public Collection<JnksIotResourceInfo> getUsedResources(TenantId tenantId, Dashboard dashboard) {
        return getUsedResources(tenantId, List.of(dashboard.getConfiguration()), List.of(DASHBOARD_RESOURCES_MAPPING)).values();
    }

    @Override
    public Collection<JnksIotResourceInfo> getUsedResources(TenantId tenantId, WidgetTypeDetails widgetTypeDetails) {
        List<JsonNode> jsonNodes = new ArrayList<>(2);
        List<Map<String, String>> mappings = new ArrayList<>(2);

        jsonNodes.add(widgetTypeDetails.getDescriptor());
        mappings.add(WIDGET_RESOURCES_MAPPING);

        JsonNode defaultConfig = widgetTypeDetails.getDefaultConfig();
        if (defaultConfig != null) {
            jsonNodes.add(defaultConfig);
            mappings.add(WIDGET_DEFAULT_CONFIG_RESOURCES_MAPPING);
        }

        return getUsedResources(tenantId, jsonNodes, mappings).values();
    }

    private Map<JnksIotResourceId, JnksIotResourceInfo> getUsedResources(TenantId tenantId, List<JsonNode> jsonNodes, List<Map<String, String>> mappings) {
        Map<JnksIotResourceId, JnksIotResourceInfo> resources = new HashMap<>();
        log.trace("[{}] getUsedResources for {}", tenantId, jsonNodes);
        processResources(jsonNodes, mappings, value -> {
            String link = getResourceLink(value);
            if (link == null) {
                return value;
            }

            ResourceType resourceType;
            String resourceKey;
            TenantId resourceTenantId;
            try {
                String[] parts = StringUtils.removeStart(link, "/api/resource/").split("/");
                resourceType = ResourceType.valueOf(parts[0].toUpperCase());
                String scope = parts[1];
                resourceKey = parts[2];
                resourceTenantId = scope.equals("system") ? TenantId.SYS_TENANT_ID : tenantId;
            } catch (Exception e) {
                log.warn("[{}] Invalid resource link '{}'", tenantId, value);
                return value;
            }

            JnksIotResourceInfo resourceInfo = findResourceInfoByTenantIdAndKey(resourceTenantId, resourceType, resourceKey);
            if (resourceInfo != null) {
                resources.putIfAbsent(resourceInfo.getId(), resourceInfo);
            } else {
                log.warn("[{}] Unknown resource referenced with '{}'", tenantId, value);
            }
            return value;
        });
        return resources;
    }

    private String getResourceLink(String value) {
        if (StringUtils.startsWith(value, DataConstants.JNKS_IOT_RESOURCE_PREFIX + "/api/resource/")) {
            return StringUtils.removeStart(value, DataConstants.JNKS_IOT_RESOURCE_PREFIX);
        } else {
            return null;
        }
    }

    private boolean processResources(List<JsonNode> jsonNodes, List<Map<String, String>> mappings, UnaryOperator<String> processor) {
        AtomicBoolean updated = new AtomicBoolean(false);

        for (int i = 0; i < jsonNodes.size(); i++) {
            JsonNode jsonNode = jsonNodes.get(i);
            // processing by mappings first
            if (i <= mappings.size() - 1) {
                JacksonUtil.replaceByMapping(jsonNode, mappings.get(i), Collections.emptyMap(), (name, urlNode) -> {
                    String value = null;
                    if (urlNode.isTextual()) { // link is in the right place
                        value = urlNode.asText();
                    } else {
                        JsonNode id = urlNode.get("id"); // old structure is used
                        if (id != null && id.isTextual()) {
                            value = id.asText();
                        }
                    }

                    if (StringUtils.isNotBlank(value)) {
                        value = processor.apply(value);
                    } else {
                        value = "";
                    }

                    JsonNode newValue = new TextNode(value);
                    if (!newValue.toString().equals(urlNode.toString())) {
                        updated.set(true);
                        log.trace("Replaced by mapping '{}' ({}) with '{}'", value, name, newValue);
                    }
                    return newValue;
                });
            }


            // processing all
            JacksonUtil.replaceAll(jsonNode, "", (name, value) -> {
                if (!StringUtils.startsWith(value, DataConstants.JNKS_IOT_RESOURCE_PREFIX + "/api/resource/")) {
                    return value;
                }

                String newValue = processor.apply(value);
                if (StringUtils.equals(value, newValue)) {
                    return value;
                } else {
                    updated.set(true);
                    log.trace("Replaced '{}' ({}) with '{}'", value, name, newValue);
                    return newValue;
                }
            });
        }

        return updated.get();
    }

    @Override
    public JnksIotResource createOrUpdateSystemResource(ResourceType resourceType, ResourceSubType resourceSubType, String resourceKey, byte[] data) {
        if (resourceType == ResourceType.DASHBOARD) {
            Dashboard dashboard = JacksonUtil.fromBytes(data, Dashboard.class);
            dashboard.setTenantId(TenantId.SYS_TENANT_ID);
            if (CollectionUtils.isNotEmpty(dashboard.getResources())) {
                importResources(dashboard.getTenantId(), dashboard.getResources());
            }
            imageService.updateImagesUsage(dashboard);
            updateResourcesUsage(dashboard.getTenantId(), dashboard);

            data = JacksonUtil.writeValueAsBytes(dashboard);
        }

        JnksIotResource resource = findResourceByTenantIdAndKey(TenantId.SYS_TENANT_ID, resourceType, resourceKey);
        if (resource == null) {
            resource = new JnksIotResource();
            resource.setTenantId(TenantId.SYS_TENANT_ID);
            resource.setResourceType(resourceType);
            resource.setResourceSubType(resourceSubType);
            resource.setResourceKey(resourceKey);
            resource.setFileName(resourceKey);
            resource.setTitle(resourceKey);
        }
        resource.setData(data);
        log.info("{} system resource {}", (resource.getId() == null ? "Creating" : "Updating"), resourceKey);
        return saveResource(resource);
    }

    @Override
    public String calculateEtag(byte[] data) {
        return Hashing.sha256().hashBytes(data).toString();
    }

    @Override
    public JnksIotResourceInfo findSystemOrTenantResourceByEtag(TenantId tenantId, ResourceType resourceType, String etag) {
        if (StringUtils.isEmpty(etag)) {
            return null;
        }
        log.trace("Executing findSystemOrTenantResourceByEtag [{}] [{}] [{}]", tenantId, resourceType, etag);
        return resourceInfoDao.findSystemOrTenantResourceByEtag(tenantId, resourceType, etag);
    }

    protected String encode(String data) {
        return encode(data.getBytes(StandardCharsets.UTF_8));
    }

    protected String encode(byte[] data) {
        if (data == null || data.length == 0) {
            return "";
        }
        return Base64.getEncoder().encodeToString(data);
    }

    protected String decode(String value) {
        if (value == null) {
            return null;
        }
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private final PaginatedRemover<TenantId, JnksIotResourceId> tenantResourcesRemover = new PaginatedRemover<>() {

        @Override
        protected PageData<JnksIotResourceId> findEntities(TenantId tenantId, TenantId id, PageLink pageLink) {
            return resourceDao.findIdsByTenantId(id.getId(), pageLink);
        }

        @Override
        protected void removeEntity(TenantId tenantId, JnksIotResourceId resourceId) {
            deleteResource(tenantId, resourceId, true);
        }
    };

    @TransactionalEventListener(classes = ResourceInfoEvictEvent.class)
    @Override
    public void handleEvictEvent(ResourceInfoEvictEvent event) {
        if (event.getResourceId() != null) {
            cache.evict(new ResourceInfoCacheKey(event.getTenantId(), event.getResourceId()));
        }
    }

}
