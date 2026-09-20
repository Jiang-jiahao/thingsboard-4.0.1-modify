package com.jnks.iot.server.dao.sql.cf;

import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.cf.CalculatedFieldDao;
import com.jnks.iot.server.dao.model.sql.CalculatedFieldEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@AllArgsConstructor
@SqlDao
public class JpaCalculatedFieldDao extends JpaAbstractDao<CalculatedFieldEntity, CalculatedField> implements CalculatedFieldDao {

    private final CalculatedFieldRepository calculatedFieldRepository;
    private final NativeCalculatedFieldRepository nativeCalculatedFieldRepository;

    @Override
    public List<CalculatedField> findAllByTenantId(TenantId tenantId) {
        return DaoUtil.convertDataList(calculatedFieldRepository.findAllByTenantId(tenantId.getId()));
    }

    @Override
    public List<CalculatedFieldId> findCalculatedFieldIdsByEntityId(TenantId tenantId, EntityId entityId) {
        return calculatedFieldRepository.findCalculatedFieldIdsByTenantIdAndEntityId(tenantId.getId(), entityId.getId());
    }

    @Override
    public List<CalculatedField> findCalculatedFieldsByEntityId(TenantId tenantId, EntityId entityId) {
        return DaoUtil.convertDataList(calculatedFieldRepository.findAllByTenantIdAndEntityId(tenantId.getId(), entityId.getId()));
    }

    @Override
    public List<CalculatedField> findAll() {
        return DaoUtil.convertDataList(calculatedFieldRepository.findAll());
    }

    @Override
    public PageData<CalculatedField> findAll(PageLink pageLink) {
        log.debug("Try to find calculated fields by pageLink [{}]", pageLink);
        return nativeCalculatedFieldRepository.findCalculatedFields(DaoUtil.toPageable(pageLink));
    }

    @Override
    public PageData<CalculatedField> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        log.debug("Try to find calculated fields by tenantId[{}] and pageLink [{}]", tenantId, pageLink);
        return DaoUtil.toPageData(calculatedFieldRepository.findAllByTenantId(tenantId.getId(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<CalculatedField> findAllByEntityId(TenantId tenantId, EntityId entityId, PageLink pageLink) {
        log.debug("Try to find calculated fields by entityId[{}] and pageLink [{}]", entityId, pageLink);
        return DaoUtil.toPageData(calculatedFieldRepository.findAllByTenantIdAndEntityId(tenantId.getId(), entityId.getId(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    @Transactional
    public List<CalculatedField> removeAllByEntityId(TenantId tenantId, EntityId entityId) {
        return DaoUtil.convertDataList(calculatedFieldRepository.removeAllByTenantIdAndEntityId(tenantId.getId(), entityId.getId()));
    }

    @Override
    public long countCFByEntityId(TenantId tenantId, EntityId entityId) {
        return calculatedFieldRepository.countByTenantIdAndEntityId(tenantId.getId(), entityId.getId());
    }

    @Override
    protected Class<CalculatedFieldEntity> getEntityClass() {
        return CalculatedFieldEntity.class;
    }

    @Override
    protected JpaRepository<CalculatedFieldEntity, UUID> getRepository() {
        return calculatedFieldRepository;
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.CALCULATED_FIELD;
    }

}
