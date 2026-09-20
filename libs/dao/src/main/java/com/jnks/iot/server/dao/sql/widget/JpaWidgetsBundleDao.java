package com.jnks.iot.server.dao.sql.widget;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.EntityFields;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.WidgetsBundleId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.widget.WidgetsBundle;
import com.jnks.iot.server.common.data.widget.WidgetsBundleFilter;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.model.sql.WidgetsBundleEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;
import com.jnks.iot.server.dao.widget.WidgetsBundleDao;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.jnks.iot.server.dao.model.ModelConstants.NULL_UUID;

/**
 * Created by Valerii Sosliuk on 4/23/2017.
 */
@Component
@SqlDao
public class JpaWidgetsBundleDao extends JpaAbstractDao<WidgetsBundleEntity, WidgetsBundle> implements WidgetsBundleDao, TenantEntityDao<WidgetsBundle> {

    @Autowired
    private WidgetsBundleRepository widgetsBundleRepository;

    @Override
    protected Class<WidgetsBundleEntity> getEntityClass() {
        return WidgetsBundleEntity.class;
    }

    @Override
    protected JpaRepository<WidgetsBundleEntity, UUID> getRepository() {
        return widgetsBundleRepository;
    }

    @Override
    public WidgetsBundle findWidgetsBundleByTenantIdAndAlias(UUID tenantId, String alias) {
        return DaoUtil.getData(widgetsBundleRepository.findWidgetsBundleByTenantIdAndAlias(tenantId, alias));
    }

    @Override
    public PageData<WidgetsBundle> findSystemWidgetsBundles(WidgetsBundleFilter widgetsBundleFilter, PageLink pageLink) {
        if (widgetsBundleFilter.isFullSearch()) {
            return DaoUtil.toPageData(
                    widgetsBundleRepository
                            .findSystemWidgetsBundlesFullSearch(
                                    NULL_UUID,
                                    pageLink.getTextSearch(),
                                    DaoUtil.toPageable(pageLink)));
        } else {
            return DaoUtil.toPageData(
                    widgetsBundleRepository
                            .findSystemWidgetsBundles(
                                    NULL_UUID,
                                    pageLink.getTextSearch(),
                                    DaoUtil.toPageable(pageLink)));
        }
    }

    @Override
    public PageData<WidgetsBundle> findTenantWidgetsBundlesByTenantId(UUID tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(
                widgetsBundleRepository
                        .findTenantWidgetsBundlesByTenantId(
                                tenantId,
                                pageLink.getTextSearch(),
                                DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<WidgetsBundle> findAllTenantWidgetsBundlesByTenantId(WidgetsBundleFilter widgetsBundleFilter, PageLink pageLink) {
        return findTenantWidgetsBundlesByTenantIds(Arrays.asList(widgetsBundleFilter.getTenantId().getId(), NULL_UUID), widgetsBundleFilter, pageLink);
    }

    @Override
    public PageData<WidgetsBundle> findTenantWidgetsBundlesByTenantId(WidgetsBundleFilter widgetsBundleFilter, PageLink pageLink) {
        return findTenantWidgetsBundlesByTenantIds(Collections.singletonList(widgetsBundleFilter.getTenantId().getId()), widgetsBundleFilter, pageLink);
    }

    @Override
    public PageData<WidgetsBundle> findAllWidgetsBundles(PageLink pageLink) {
        return DaoUtil.toPageData(widgetsBundleRepository.findAll(DaoUtil.toPageable(pageLink)));
    }

    private PageData<WidgetsBundle> findTenantWidgetsBundlesByTenantIds(List<UUID> tenantIds, WidgetsBundleFilter widgetsBundleFilter, PageLink pageLink) {
        if (widgetsBundleFilter.isFullSearch()) {
            return DaoUtil.toPageData(
                    widgetsBundleRepository
                            .findAllTenantWidgetsBundlesByTenantIdsFullSearch(
                                    tenantIds,
                                    pageLink.getTextSearch(),
                                    widgetsBundleFilter.isScadaFirst(),
                                    DaoUtil.toPageable(pageLink)));
        } else {
            return DaoUtil.toPageData(
                    widgetsBundleRepository
                            .findAllTenantWidgetsBundlesByTenantIds(
                                    tenantIds,
                                    pageLink.getTextSearch(),
                                    DaoUtil.toPageable(pageLink)));
        }
    }

    @Override
    public WidgetsBundle findByTenantIdAndExternalId(UUID tenantId, UUID externalId) {
        return DaoUtil.getData(widgetsBundleRepository.findByTenantIdAndExternalId(tenantId, externalId));
    }

    @Override
    public WidgetsBundle findByTenantIdAndName(UUID tenantId, String name) {
        return DaoUtil.getData(widgetsBundleRepository.findFirstByTenantIdAndTitle(tenantId, name));
    }

    @Override
    public PageData<WidgetsBundle> findByTenantId(UUID tenantId, PageLink pageLink) {
        return findTenantWidgetsBundlesByTenantId(tenantId, pageLink);
    }

    @Override
    public WidgetsBundleId getExternalIdByInternal(WidgetsBundleId internalId) {
        return Optional.ofNullable(widgetsBundleRepository.getExternalIdById(internalId.getId()))
                .map(WidgetsBundleId::new).orElse(null);
    }

    @Override
    public List<WidgetsBundle> findByTenantAndImageLink(TenantId tenantId, String imageUrl, int limit) {
        return DaoUtil.convertDataList(widgetsBundleRepository.findByTenantAndImageUrl(tenantId.getId(), imageUrl, limit));
    }

    @Override
    public List<WidgetsBundle> findByImageLink(String imageUrl, int limit) {
        return DaoUtil.convertDataList(widgetsBundleRepository.findByImageUrl(imageUrl, limit));
    }

    @Override
    public PageData<WidgetsBundle> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return findByTenantId(tenantId.getId(), pageLink);
    }

    @Override
    public List<? extends EntityFields> findNextBatch(UUID id, int batchSize) {
        return widgetsBundleRepository.findNextBatch(id, Limit.of(batchSize));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.WIDGETS_BUNDLE;
    }

}
