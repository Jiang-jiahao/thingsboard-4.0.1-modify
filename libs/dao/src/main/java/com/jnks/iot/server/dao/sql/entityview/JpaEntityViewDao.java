package com.jnks.iot.server.dao.sql.entityview;

import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntitySubtype;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.common.data.EntityViewInfo;
import com.jnks.iot.server.common.data.edqs.fields.EntityViewFields;
import com.jnks.iot.server.common.data.id.EntityViewId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.entityview.EntityViewDao;
import com.jnks.iot.server.dao.model.sql.EntityViewEntity;
import com.jnks.iot.server.dao.model.sql.EntityViewInfoEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.jnks.iot.server.dao.DaoUtil.convertTenantEntityTypesToDto;

/**
 * Created by Victor Basanets on 8/31/2017.
 */
@Component
@Slf4j
@SqlDao
public class JpaEntityViewDao extends JpaAbstractDao<EntityViewEntity, EntityView> implements EntityViewDao, TenantEntityDao<EntityView> {

    @Autowired
    private EntityViewRepository entityViewRepository;

    @Override
    protected Class<EntityViewEntity> getEntityClass() {
        return EntityViewEntity.class;
    }

    @Override
    protected JpaRepository<EntityViewEntity, UUID> getRepository() {
        return entityViewRepository;
    }

    @Override
    public EntityViewInfo findEntityViewInfoById(TenantId tenantId, UUID entityViewId) {
        return DaoUtil.getData(entityViewRepository.findEntityViewInfoById(entityViewId));
    }

    @Override
    public PageData<EntityView> findEntityViewsByTenantId(UUID tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(
                entityViewRepository.findByTenantId(
                        tenantId,
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<EntityViewInfo> findEntityViewInfosByTenantId(UUID tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(
                entityViewRepository.findEntityViewInfosByTenantId(
                        tenantId,
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink, EntityViewInfoEntity.entityViewInfoColumnMap)));
    }

    @Override
    public PageData<EntityView> findEntityViewsByTenantIdAndType(UUID tenantId, String type, PageLink pageLink) {
        return DaoUtil.toPageData(
                entityViewRepository.findByTenantIdAndType(
                        tenantId,
                        type,
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<EntityViewInfo> findEntityViewInfosByTenantIdAndType(UUID tenantId, String type, PageLink pageLink) {
        return DaoUtil.toPageData(
                entityViewRepository.findEntityViewInfosByTenantIdAndType(
                        tenantId,
                        type,
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink, EntityViewInfoEntity.entityViewInfoColumnMap)));
    }

    @Override
    public Optional<EntityView> findEntityViewByTenantIdAndName(UUID tenantId, String name) {
        return Optional.ofNullable(
                DaoUtil.getData(entityViewRepository.findByTenantIdAndName(tenantId, name)));
    }

    @Override
    public PageData<EntityView> findEntityViewsByTenantIdAndCustomerId(UUID tenantId,
                                                                       UUID customerId,
                                                                       PageLink pageLink) {
        return DaoUtil.toPageData(
                entityViewRepository.findByTenantIdAndCustomerId(
                        tenantId,
                        customerId,
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink)
                ));
    }

    @Override
    public PageData<EntityViewInfo> findEntityViewInfosByTenantIdAndCustomerId(UUID tenantId, UUID customerId, PageLink pageLink) {
        return DaoUtil.toPageData(
                entityViewRepository.findEntityViewInfosByTenantIdAndCustomerId(
                        tenantId,
                        customerId,
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink, EntityViewInfoEntity.entityViewInfoColumnMap)));
    }

    @Override
    public PageData<EntityView> findEntityViewsByTenantIdAndCustomerIdAndType(UUID tenantId, UUID customerId, String type, PageLink pageLink) {
        return DaoUtil.toPageData(
                entityViewRepository.findByTenantIdAndCustomerIdAndType(
                        tenantId,
                        customerId,
                        type,
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink)
                ));
    }

    @Override
    public PageData<EntityViewInfo> findEntityViewInfosByTenantIdAndCustomerIdAndType(UUID tenantId, UUID customerId, String type, PageLink pageLink) {
        return DaoUtil.toPageData(
                entityViewRepository.findEntityViewInfosByTenantIdAndCustomerIdAndType(
                        tenantId,
                        customerId,
                        type,
                        pageLink.getTextSearch(),
                        DaoUtil.toPageable(pageLink, EntityViewInfoEntity.entityViewInfoColumnMap)));
    }

    @Override
    public List<EntityView> findEntityViewsByTenantIdAndEntityId(UUID tenantId, UUID entityId) {
        return DaoUtil.convertDataList(
                entityViewRepository.findAllByTenantIdAndEntityId(tenantId, entityId));
    }

    @Override
    public boolean existsByTenantIdAndEntityId(UUID tenantId, UUID entityId) {
        return entityViewRepository.existsByTenantIdAndEntityId(tenantId, entityId);
    }

    @Override
    public ListenableFuture<List<EntitySubtype>> findTenantEntityViewTypesAsync(UUID tenantId) {
        return service.submit(() -> convertTenantEntityTypesToDto(tenantId, EntityType.ENTITY_VIEW, entityViewRepository.findTenantEntityViewTypes(tenantId)));
    }

    @Override
    public EntityView findByTenantIdAndExternalId(UUID tenantId, UUID externalId) {
        return DaoUtil.getData(entityViewRepository.findByTenantIdAndExternalId(tenantId, externalId));
    }

    @Override
    public PageData<EntityView> findByTenantId(UUID tenantId, PageLink pageLink) {
        return findEntityViewsByTenantId(tenantId, pageLink);
    }

    @Override
    public EntityViewId getExternalIdByInternal(EntityViewId internalId) {
        return Optional.ofNullable(entityViewRepository.getExternalIdById(internalId.getId()))
                .map(EntityViewId::new).orElse(null);
    }

    @Override
    public EntityView findByTenantIdAndName(UUID tenantId, String name) {
        return findEntityViewByTenantIdAndName(tenantId, name).orElse(null);
    }

    @Override
    public PageData<EntityView> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return findByTenantId(tenantId.getId(), pageLink);
    }

    @Override
    public List<EntityViewFields> findNextBatch(UUID id, int batchSize) {
        return entityViewRepository.findNextBatch(id, Limit.of(batchSize));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.ENTITY_VIEW;
    }

}
