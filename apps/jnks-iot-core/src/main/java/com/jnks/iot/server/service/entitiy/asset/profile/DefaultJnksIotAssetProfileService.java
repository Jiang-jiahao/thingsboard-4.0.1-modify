package com.jnks.iot.server.service.entitiy.asset.profile;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.AssetProfileId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.asset.AssetProfileService;
import com.jnks.iot.server.service.entitiy.AbstractJnksIotEntityService;
import com.jnks.iot.server.service.security.model.SecurityUser;

/**
 * {@link JnksIotAssetProfileService} 的默认实现。
 * <p>
 * 由 AssetProfileController 调用，委托 {@link AssetProfileService} 落库；写审计日志，保存时 autoCommit。
 *
 * @see JnksIotAssetProfileService
 */
@Service
@AllArgsConstructor
@Slf4j
public class DefaultJnksIotAssetProfileService extends AbstractJnksIotEntityService implements JnksIotAssetProfileService {

    private final AssetProfileService assetProfileService;

    /** 保存资产配置，写审计并尝试 autoCommit。 */
    @Override
    public AssetProfile save(AssetProfile assetProfile, SecurityUser user) throws Exception {
        ActionType actionType = assetProfile.getId() == null ? ActionType.ADDED : ActionType.UPDATED;
        TenantId tenantId = assetProfile.getTenantId();
        try {
            AssetProfile savedAssetProfile = checkNotNull(assetProfileService.saveAssetProfile(assetProfile));
            autoCommit(user, savedAssetProfile.getId());
            logEntityActionService.logEntityAction(tenantId, savedAssetProfile.getId(), savedAssetProfile,
                    null, actionType, user);
            return savedAssetProfile;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.ASSET_PROFILE), assetProfile, actionType, user, e);
            throw e;
        }
    }

    /** 删除资产配置并写 DELETED 审计。 */
    @Override
    public void delete(AssetProfile assetProfile, User user) {
        ActionType actionType = ActionType.DELETED;
        AssetProfileId assetProfileId = assetProfile.getId();
        TenantId tenantId = assetProfile.getTenantId();
        try {
            assetProfileService.deleteAssetProfile(tenantId, assetProfileId);
            logEntityActionService.logEntityAction(tenantId, assetProfileId, assetProfile, null,
                    actionType, user, assetProfileId.toString());
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.ASSET_PROFILE), actionType,
                    user, e, assetProfileId.toString());
            throw e;
        }
    }

    /** 设为默认资产配置，并对新旧配置写 UPDATED 审计。 */
    @Override
    public AssetProfile setDefaultAssetProfile(AssetProfile assetProfile, AssetProfile previousDefaultAssetProfile, User user) throws JnksIotException {
        TenantId tenantId = assetProfile.getTenantId();
        AssetProfileId assetProfileId = assetProfile.getId();
        try {
            if (assetProfileService.setDefaultAssetProfile(tenantId, assetProfileId)) {
                if (previousDefaultAssetProfile != null) {
                    previousDefaultAssetProfile = assetProfileService.findAssetProfileById(tenantId, previousDefaultAssetProfile.getId());
                    logEntityActionService.logEntityAction(tenantId, previousDefaultAssetProfile.getId(), previousDefaultAssetProfile,
                            ActionType.UPDATED, user);
                }
                assetProfile = assetProfileService.findAssetProfileById(tenantId, assetProfileId);
                logEntityActionService.logEntityAction(tenantId, assetProfileId, assetProfile, ActionType.UPDATED, user);
            }
            return assetProfile;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.ASSET_PROFILE), ActionType.UPDATED,
                    user, e, assetProfileId.toString());
            throw e;
        }
    }
}
