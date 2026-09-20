package com.jnks.iot.server.dao.service.validator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.DashboardInfo;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.common.data.rule.RuleChain;
import com.jnks.iot.server.dao.asset.AssetProfileDao;
import com.jnks.iot.server.dao.asset.AssetProfileService;
import com.jnks.iot.server.dao.dashboard.DashboardService;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.queue.QueueService;
import com.jnks.iot.server.dao.rule.RuleChainService;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.tenant.TenantService;

@Component
public class AssetProfileDataValidator extends DataValidator<AssetProfile> {

    @Autowired
    private AssetProfileDao assetProfileDao;
    @Autowired
    @Lazy
    private AssetProfileService assetProfileService;
    @Autowired
    private TenantService tenantService;
    @Lazy
    @Autowired
    private QueueService queueService;
    @Autowired
    private RuleChainService ruleChainService;
    @Autowired
    private DashboardService dashboardService;

    @Override
    protected void validateDataImpl(TenantId tenantId, AssetProfile assetProfile) {
        validateString("Asset profile name", assetProfile.getName());
        if (assetProfile.getTenantId() == null) {
            throw new DataValidationException("Asset profile should be assigned to tenant!");
        } else {
            if (!tenantService.tenantExists(assetProfile.getTenantId())) {
                throw new DataValidationException("Asset profile is referencing to non-existent tenant!");
            }
        }
        if (assetProfile.isDefault()) {
            AssetProfile defaultAssetProfile = assetProfileService.findDefaultAssetProfile(tenantId);
            if (defaultAssetProfile != null && !defaultAssetProfile.getId().equals(assetProfile.getId())) {
                throw new DataValidationException("Another default asset profile is present in scope of current tenant!");
            }
        }
        if (StringUtils.isNotEmpty(assetProfile.getDefaultQueueName())) {
            Queue queue = queueService.findQueueByTenantIdAndName(tenantId, assetProfile.getDefaultQueueName());
            if (queue == null) {
                throw new DataValidationException("Asset profile is referencing to non-existent queue!");
            }
        }

        if (assetProfile.getDefaultRuleChainId() != null) {
            RuleChain ruleChain = ruleChainService.findRuleChainById(tenantId, assetProfile.getDefaultRuleChainId());
            if (ruleChain == null) {
                throw new DataValidationException("Can't assign non-existent rule chain!");
            }
            if (!ruleChain.getTenantId().equals(assetProfile.getTenantId())) {
                throw new DataValidationException("Can't assign rule chain from different tenant!");
            }
        }

        if (assetProfile.getDefaultDashboardId() != null) {
            DashboardInfo dashboard = dashboardService.findDashboardInfoById(tenantId, assetProfile.getDefaultDashboardId());
            if (dashboard == null) {
                throw new DataValidationException("Can't assign non-existent dashboard!");
            }
            if (!dashboard.getTenantId().equals(assetProfile.getTenantId())) {
                throw new DataValidationException("Can't assign dashboard from different tenant!");
            }
        }
    }

    @Override
    protected AssetProfile validateUpdate(TenantId tenantId, AssetProfile assetProfile) {
        AssetProfile old = assetProfileDao.findById(assetProfile.getTenantId(), assetProfile.getId().getId());
        if (old == null) {
            throw new DataValidationException("Can't update non existing asset profile!");
        }
        return old;
    }

}
