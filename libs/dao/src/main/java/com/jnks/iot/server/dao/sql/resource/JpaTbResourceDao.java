package com.jnks.iot.server.dao.sql.resource;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.ResourceSubType;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.TbResource;
import com.jnks.iot.server.common.data.id.TbResourceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.model.sql.TbResourceEntity;
import com.jnks.iot.server.dao.resource.TbResourceDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@SqlDao
public class JpaTbResourceDao extends JpaAbstractDao<TbResourceEntity, TbResource> implements TbResourceDao, TenantEntityDao<TbResource> {

    private final TbResourceRepository resourceRepository;

    public JpaTbResourceDao(TbResourceRepository resourceRepository) {
        this.resourceRepository = resourceRepository;
    }

    @Override
    protected Class<TbResourceEntity> getEntityClass() {
        return TbResourceEntity.class;
    }

    @Override
    protected JpaRepository<TbResourceEntity, UUID> getRepository() {
        return resourceRepository;
    }

    @Override
    public TbResource findResourceByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceKey) {
        return DaoUtil.getData(resourceRepository.findByTenantIdAndResourceTypeAndResourceKey(tenantId.getId(), resourceType.name(), resourceKey));
    }

    @Override
    public PageData<TbResource> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(resourceRepository.findAllByTenantId(tenantId.getId(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<TbResource> findResourcesByTenantIdAndResourceType(TenantId tenantId,
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
    public List<TbResource> findResourcesByTenantIdAndResourceType(TenantId tenantId, ResourceType resourceType,
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
    public byte[] getResourceData(TenantId tenantId, TbResourceId resourceId) {
        return resourceRepository.getDataById(resourceId.getId());
    }

    @Override
    public byte[] getResourcePreview(TenantId tenantId, TbResourceId resourceId) {
        return resourceRepository.getPreviewById(resourceId.getId());
    }

    @Override
    public long getResourceSize(TenantId tenantId, TbResourceId resourceId) {
        return resourceRepository.getDataSizeById(resourceId.getId());
    }

    @Override
    public Long sumDataSizeByTenantId(TenantId tenantId) {
        return resourceRepository.sumDataSizeByTenantId(tenantId.getId());
    }

    @Override
    public TbResource findByTenantIdAndExternalId(UUID tenantId, UUID externalId) {
        return DaoUtil.getData(resourceRepository.findByTenantIdAndExternalId(tenantId, externalId));
    }

    @Override
    public PageData<TbResource> findByTenantId(UUID tenantId, PageLink pageLink) {
        return findAllByTenantId(TenantId.fromUUID(tenantId), pageLink);
    }

    @Override
    public PageData<TbResourceId> findIdsByTenantId(UUID tenantId, PageLink pageLink) {
        return DaoUtil.pageToPageData(resourceRepository.findIdsByTenantId(tenantId, DaoUtil.toPageable(pageLink))
                .map(TbResourceId::new));
    }

    @Override
    public TbResourceId getExternalIdByInternal(TbResourceId internalId) {
        return DaoUtil.toEntityId(resourceRepository.getExternalIdByInternal(internalId.getId()), TbResourceId::new);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.TB_RESOURCE;
    }

}
