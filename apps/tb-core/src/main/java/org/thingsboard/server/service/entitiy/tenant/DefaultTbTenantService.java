package org.thingsboard.server.service.entitiy.tenant;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.dao.tenant.TenantProfileService;
import org.thingsboard.server.dao.tenant.TenantService;
import org.thingsboard.server.service.entitiy.AbstractTbEntityService;
import org.thingsboard.server.service.entitiy.queue.TbQueueService;
import org.thingsboard.server.service.install.InstallScripts;
import org.thingsboard.server.service.sync.vc.EntitiesVersionControlService;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link TbTenantService} 的默认实现。
 * <p>
 * 由 TenantController 调用，委托 {@link TenantService} 落库；新建时安装默认规则链/仪表板，
 * 刷新租户配置缓存，并按配置同步队列。删除时清缓存并删除版本控制设置。
 *
 * @see TbTenantService
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DefaultTbTenantService extends AbstractTbEntityService implements TbTenantService {

    private final TenantService tenantService;
    private final TbTenantProfileCache tenantProfileCache;
    private final InstallScripts installScripts;
    private final TbQueueService tbQueueService;
    private final TenantProfileService tenantProfileService;
    private final EntitiesVersionControlService versionControlService;

    /** 保存租户；新建时安装默认规则链与仪表板，并同步队列。 */
    @Override
    public Tenant save(Tenant tenant) throws Exception {
        boolean created = tenant.getId() == null;
        Tenant oldTenant = !created ? tenantService.findTenantById(tenant.getId()) : null;

        Tenant savedTenant = tenantService.saveTenant(tenant, tenantId -> {
            installScripts.createDefaultRuleChains(tenantId);
            if (!isTestProfile()) {
                installScripts.createDefaultTenantDashboards(tenantId, null);
            }
        });
        tenantProfileCache.evict(savedTenant.getId());

        TenantProfile oldTenantProfile = oldTenant != null ? tenantProfileService.findTenantProfileById(TenantId.SYS_TENANT_ID, oldTenant.getTenantProfileId()) : null;
        TenantProfile newTenantProfile = tenantProfileService.findTenantProfileById(TenantId.SYS_TENANT_ID, savedTenant.getTenantProfileId());
        tbQueueService.updateQueuesByTenants(Collections.singletonList(savedTenant.getTenantId()), newTenantProfile, oldTenantProfile);
        return savedTenant;
    }

    /** 删除租户、配置缓存与版本控制设置。 */
    @Override
    public void delete(Tenant tenant) throws Exception {
        TenantId tenantId = tenant.getId();
        tenantService.deleteTenant(tenantId);
        tenantProfileCache.evict(tenantId);
        // 版本控制仓库的清理由独立的 VC 执行器响应，该执行器未部署时会一直等不到响应。
        // 此时租户本体已经删除，不能因为这份尽力而为的清理没做完就让接口以 500 收场。
        try {
            versionControlService.deleteVersionControlSettings(tenantId).get(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (TimeoutException | ExecutionException e) {
            log.warn("[{}] Version control settings cleanup did not complete: {}. Tenant itself is already deleted.",
                    tenantId, e.getMessage());
        }
    }
}
