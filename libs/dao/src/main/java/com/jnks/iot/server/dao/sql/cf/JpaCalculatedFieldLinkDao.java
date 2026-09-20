package com.jnks.iot.server.dao.sql.cf;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.cf.CalculatedFieldLink;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.cf.CalculatedFieldLinkDao;
import com.jnks.iot.server.dao.model.sql.CalculatedFieldLinkEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@AllArgsConstructor
@SqlDao
public class JpaCalculatedFieldLinkDao extends JpaAbstractDao<CalculatedFieldLinkEntity, CalculatedFieldLink> implements CalculatedFieldLinkDao {

    private final CalculatedFieldLinkRepository calculatedFieldLinkRepository;
    private final NativeCalculatedFieldRepository nativeCalculatedFieldRepository;

    @Override
    public List<CalculatedFieldLink> findCalculatedFieldLinksByCalculatedFieldId(TenantId tenantId, CalculatedFieldId calculatedFieldId) {
        return DaoUtil.convertDataList(calculatedFieldLinkRepository.findAllByTenantIdAndCalculatedFieldId(tenantId.getId(), calculatedFieldId.getId()));
    }

    @Override
    public List<CalculatedFieldLink> findCalculatedFieldLinksByEntityId(TenantId tenantId, EntityId entityId) {
        return DaoUtil.convertDataList(calculatedFieldLinkRepository.findAllByTenantIdAndEntityId(tenantId.getId(), entityId.getId()));
    }

    @Override
    public List<CalculatedFieldLink> findCalculatedFieldLinksByTenantId(TenantId tenantId) {
        return DaoUtil.convertDataList(calculatedFieldLinkRepository.findAllByTenantId(tenantId.getId()));
    }

    @Override
    public List<CalculatedFieldLink> findAll() {
        return DaoUtil.convertDataList(calculatedFieldLinkRepository.findAll());
    }

    @Override
    public PageData<CalculatedFieldLink> findAll(PageLink pageLink) {
        log.debug("Try to find calculated field links by pageLink [{}]", pageLink);
        return nativeCalculatedFieldRepository.findCalculatedFieldLinks(DaoUtil.toPageable(pageLink));
    }

    @Override
    public PageData<CalculatedFieldLink> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        log.debug("Try to find calculated field links by tenantId [{}], pageLink [{}]", tenantId, pageLink);
        return DaoUtil.toPageData(calculatedFieldLinkRepository.findAllByTenantId(tenantId.getId(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    protected Class<CalculatedFieldLinkEntity> getEntityClass() {
        return CalculatedFieldLinkEntity.class;
    }

    @Override
    protected JpaRepository<CalculatedFieldLinkEntity, UUID> getRepository() {
        return calculatedFieldLinkRepository;
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.CALCULATED_FIELD_LINK;
    }

}
