package com.jnks.iot.server.dao.sql.resource;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.ResourceSubType;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.TbResourceInfo;
import com.jnks.iot.server.common.data.TbResourceInfoFilter;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.util.CollectionsUtil;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.model.sql.TbResourceInfoEntity;
import com.jnks.iot.server.dao.resource.TbResourceInfoDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Component
@SqlDao
public class JpaTbResourceInfoDao extends JpaAbstractDao<TbResourceInfoEntity, TbResourceInfo> implements TbResourceInfoDao {

    @Autowired
    private TbResourceInfoRepository resourceInfoRepository;

    @Override
    protected Class<TbResourceInfoEntity> getEntityClass() {
        return TbResourceInfoEntity.class;
    }

    @Override
    protected JpaRepository<TbResourceInfoEntity, UUID> getRepository() {
        return resourceInfoRepository;
    }

    @Override
    public PageData<TbResourceInfo> findAllTenantResourcesByTenantId(TbResourceInfoFilter filter, PageLink pageLink) {
        Set<ResourceType> resourceTypes = filter.getResourceTypes();
        if (CollectionsUtil.isEmpty(resourceTypes)) {
            resourceTypes = EnumSet.allOf(ResourceType.class);
        }
        Set<ResourceSubType> resourceSubTypes = filter.getResourceSubTypes();
        return DaoUtil.toPageData(resourceInfoRepository
                .findAllTenantResourcesByTenantId(
                        filter.getTenantId().getId(), TenantId.NULL_UUID,
                        resourceTypes.stream().map(Enum::name).collect(Collectors.toList()),
                        CollectionsUtil.isEmpty(resourceSubTypes) ? null :
                                resourceSubTypes.stream().map(Enum::name).collect(Collectors.toList()),
                        Objects.toString(pageLink.getTextSearch(), ""),
                        DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<TbResourceInfo> findTenantResourcesByTenantId(TbResourceInfoFilter filter, PageLink pageLink) {
        Set<ResourceType> resourceTypes = filter.getResourceTypes();
        if (CollectionsUtil.isEmpty(resourceTypes)) {
            resourceTypes = EnumSet.allOf(ResourceType.class);
        }
        Set<ResourceSubType> resourceSubTypes = filter.getResourceSubTypes();
        return DaoUtil.toPageData(resourceInfoRepository
                .findTenantResourcesByTenantId(
                        filter.getTenantId().getId(),
                        resourceTypes.stream().map(Enum::name).collect(Collectors.toList()),
                        CollectionsUtil.isEmpty(resourceSubTypes) ? null :
                                resourceSubTypes.stream().map(Enum::name).collect(Collectors.toList()),
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink)));
    }

    @Override
    public TbResourceInfo findByTenantIdAndKey(TenantId tenantId, ResourceType resourceType, String resourceKey) {
        return DaoUtil.getData(resourceInfoRepository.findByTenantIdAndResourceTypeAndResourceKey(tenantId.getId(), resourceType.name(), resourceKey));
    }

    @Override
    public boolean existsByTenantIdAndResourceTypeAndResourceKey(TenantId tenantId, ResourceType resourceType, String resourceKey) {
        return resourceInfoRepository.existsByTenantIdAndResourceTypeAndResourceKey(tenantId.getId(), resourceType.name(), resourceKey);
    }

    @Override
    public Set<String> findKeysByTenantIdAndResourceTypeAndResourceKeyPrefix(TenantId tenantId, ResourceType resourceType, String prefix) {
        return resourceInfoRepository.findKeysByTenantIdAndResourceTypeAndResourceKeyStartingWith(tenantId.getId(), resourceType.name(), prefix);
    }

    @Override
    public List<TbResourceInfo> findByTenantIdAndEtagAndKeyStartingWith(TenantId tenantId, String etag, String query) {
        return DaoUtil.convertDataList(resourceInfoRepository.findByTenantIdAndEtagAndResourceKeyStartingWith(tenantId.getId(), etag, query));
    }

    @Override
    public TbResourceInfo findSystemOrTenantResourceByEtag(TenantId tenantId, ResourceType resourceType, String etag) {
        return DaoUtil.getData(resourceInfoRepository.findSystemOrTenantResourceByEtag(tenantId.getId(), resourceType.name(), etag));
    }

    @Override
    public boolean existsByPublicResourceKey(ResourceType resourceType, String publicResourceKey) {
        return resourceInfoRepository.existsByResourceTypeAndPublicResourceKey(resourceType.name(), publicResourceKey);
    }

    @Override
    public TbResourceInfo findPublicResourceByKey(ResourceType resourceType, String publicResourceKey) {
        return DaoUtil.getData(resourceInfoRepository.findByResourceTypeAndPublicResourceKeyAndIsPublicTrue(resourceType.name(), publicResourceKey));
    }
}
