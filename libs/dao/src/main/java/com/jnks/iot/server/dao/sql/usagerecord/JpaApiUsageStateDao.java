package com.jnks.iot.server.dao.sql.usagerecord;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.ApiUsageStateFields;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.model.sql.ApiUsageStateEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.usagerecord.ApiUsageStateDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.UUID;

/**
 * @author Andrii Shvaika
 */
@Component
@SqlDao
public class JpaApiUsageStateDao extends JpaAbstractDao<ApiUsageStateEntity, ApiUsageState> implements ApiUsageStateDao {

    private final ApiUsageStateRepository apiUsageStateRepository;

    public JpaApiUsageStateDao(ApiUsageStateRepository apiUsageStateRepository) {
        this.apiUsageStateRepository = apiUsageStateRepository;
    }

    @Override
    protected Class<ApiUsageStateEntity> getEntityClass() {
        return ApiUsageStateEntity.class;
    }

    @Override
    protected JpaRepository<ApiUsageStateEntity, UUID> getRepository() {
        return apiUsageStateRepository;
    }

    @Override
    public ApiUsageState findTenantApiUsageState(UUID tenantId) {
        return DaoUtil.getData(apiUsageStateRepository.findByTenantId(tenantId));
    }

    @Override
    public ApiUsageState findApiUsageStateByEntityId(EntityId entityId) {
        return DaoUtil.getData(apiUsageStateRepository.findByEntityIdAndEntityType(entityId.getId(), entityId.getEntityType().name()));
    }

    @Override
    public void deleteApiUsageStateByTenantId(TenantId tenantId) {
        apiUsageStateRepository.deleteApiUsageStateByTenantId(tenantId.getId());
    }

    @Override
    public void deleteApiUsageStateByEntityId(EntityId entityId) {
        apiUsageStateRepository.deleteByEntityIdAndEntityType(entityId.getId(), entityId.getEntityType().name());
    }

    @Override
    public PageData<ApiUsageState> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(apiUsageStateRepository.findAllByTenantId(tenantId.getId(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public List<ApiUsageStateFields> findNextBatch(UUID id, int batchSize) {
        return apiUsageStateRepository.findNextBatch(id, Limit.of(batchSize));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.API_USAGE_STATE;
    }

}
