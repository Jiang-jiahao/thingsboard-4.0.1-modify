package com.jnks.iot.server.dao.sql.resource;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.ResourceSubType;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.JnksIotResource;
import com.jnks.iot.server.common.data.id.JnksIotResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.model.sql.JnksIotResourceEntity;
import com.jnks.iot.server.dao.resource.JnksIotResourceDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@SqlDao
public class JpaJnksIotResourceDao extends JpaAbstractDao<JnksIotResourceEntity, JnksIotResource> implements JnksIotResourceDao, TenantEntityDao<JnksIotResource> {

    private final JnksIotResourceRepository resourceRepository;

    public JpaJnksIotResourceDao(JnksIotResourceRepository resourceRepository) {
        this.resourceRepository = resourceRepository;
    }

    @Override
    protected Class<JnksIotResourceEntity> getEntityClass() {
        return JnksIotResourceEntity.class;
    }

    @Override
    protected JpaRepository<JnksIotResourceEntity, UUID> getRepository() {
        return resourceRepository;
    }

    @Override
    public JnksIotResource findResourceByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceKey) {
        return DaoUtil.getData(resourceRepository.findByTenantIdAndResourceTypeAndResourceKey(tenantId.getId(), resourceType.name(), resourceKey));
    }

    @Override
    public PageData<JnksIotResource> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(resourceRepository.findAllByTenantId(tenantId.getId(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<JnksIotResource> findResourcesByTenantIdAndResourceType(TenantId tenantId,
                                                                       ResourceType resourceType,
                                                                       ResourceSubType resourceSubType,
                                                                       PageLink pageLink) {
        return DaoUtil.toPageData(resourceRepository.findResourcesPage(
                tenantId.getId(),
                TenantId.SYS_TENANT_ID.getId(),
                resourceType.name(),
                resourceSubType != null ? resourceSubType.name() : null,
                pageLink.getTextSearch(),
                DaoUtil.toPageable(pageLink)
        ));
    }

    @Override
    public List<JnksIotResource> findResourcesByTenantIdAndResourceType(TenantId tenantId, ResourceType resourceType,
                                                                   ResourceSubType resourceSubType,
                                                                   String[] objectIds,
                                                                   String searchText) {
        return objectIds == null ?
                DaoUtil.convertDataList(resourceRepository.findResources(
                        tenantId.getId(),
                        TenantId.SYS_TENANT_ID.getId(),
                        resourceType.name(),
                        resourceSubType != null ? resourceSubType.name() : null,
                        searchText)) :
                DaoUtil.convertDataList(resourceRepository.findResourcesByIds(
                        tenantId.getId(),
                        TenantId.SYS_TENANT_ID.getId(),
                        resourceType.name(), objectIds));
    }

    @Override
    public byte[] getResourceData(TenantId tenantId, JnksIotResourceId resourceId) {
        return resourceRepository.getDataById(resourceId.getId());
    }

    @Override
    public byte[] getResourcePreview(TenantId tenantId, JnksIotResourceId resourceId) {
        return resourceRepository.getPreviewById(resourceId.getId());
    }

    @Override
    public long getResourceSize(TenantId tenantId, JnksIotResourceId resourceId) {
        return resourceRepository.getDataSizeById(resourceId.getId());
    }

    @Override
    public Long sumDataSizeByTenantId(TenantId tenantId) {
        return resourceRepository.sumDataSizeByTenantId(tenantId.getId());
    }

    @Override
    public JnksIotResource findByTenantIdAndExternalId(UUID tenantId, UUID externalId) {
        return DaoUtil.getData(resourceRepository.findByTenantIdAndExternalId(tenantId, externalId));
    }

    @Override
    public PageData<JnksIotResource> findByTenantId(UUID tenantId, PageLink pageLink) {
        return findAllByTenantId(TenantId.fromUUID(tenantId), pageLink);
    }

    @Override
    public PageData<JnksIotResourceId> findIdsByTenantId(UUID tenantId, PageLink pageLink) {
        return DaoUtil.pageToPageData(resourceRepository.findIdsByTenantId(tenantId, DaoUtil.toPageable(pageLink))
                .map(JnksIotResourceId::new));
    }

    @Override
    public JnksIotResourceId getExternalIdByInternal(JnksIotResourceId internalId) {
        return DaoUtil.toEntityId(resourceRepository.getExternalIdByInternal(internalId.getId()), JnksIotResourceId::new);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.JNKS_IOT_RESOURCE;
    }

}
