package com.jnks.iot.server.service.entitiy.tenant.profile;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.tenant.TbTenantProfileCache;
import com.jnks.iot.server.dao.tenant.TenantProfileService;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.service.entitiy.AbstractTbEntityService;
import com.jnks.iot.server.service.entitiy.queue.TbQueueService;

import java.util.List;

/**
 * {@link TbTenantProfileService} 的默认实现。
 * <p>
 * 由 TenantProfileController 调用，委托 {@link TenantProfileService} 落库后刷新缓存，
 * 再经 {@link TbQueueService} 为使用该配置的租户同步队列。
 *
 * @see TbTenantProfileService
 */
@Slf4j
@Service
@AllArgsConstructor
public class DefaultTbTenantProfileService extends AbstractTbEntityService implements TbTenantProfileService {
    private final TbQueueService tbQueueService;
    private final TenantProfileService tenantProfileService;
    private final TenantService tenantService;
    private final TbTenantProfileCache tenantProfileCache;

    /** 保存租户配置，刷新缓存并更新关联租户队列。 */
    @Override
    public TenantProfile save(TenantId tenantId, TenantProfile tenantProfile, TenantProfile oldTenantProfile) throws JnksIotException {
        TenantProfile savedTenantProfile = checkNotNull(tenantProfileService.saveTenantProfile(tenantId, tenantProfile));
        tenantProfileCache.put(savedTenantProfile);

        List<TenantId> tenantIds = tenantService.findTenantIdsByTenantProfileId(savedTenantProfile.getId());
        tbQueueService.updateQueuesByTenants(tenantIds, savedTenantProfile, oldTenantProfile);

        return savedTenantProfile;
    }

    /** 删除租户配置。 */
    @Override
    public void delete(TenantId tenantId, TenantProfile tenantProfile) throws JnksIotException {
        tenantProfileService.deleteTenantProfile(tenantId, tenantProfile.getId());
    }
}
