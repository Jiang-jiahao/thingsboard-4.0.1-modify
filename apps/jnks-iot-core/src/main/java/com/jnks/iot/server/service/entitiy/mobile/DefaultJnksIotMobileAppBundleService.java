package com.jnks.iot.server.service.entitiy.mobile;

import lombok.AllArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.OAuth2ClientId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundle;
import com.jnks.iot.server.dao.mobile.MobileAppBundleService;
import com.jnks.iot.server.service.entitiy.AbstractJnksIotEntityService;

import java.util.List;

/**
 * {@link JnksIotMobileAppBundleService} 的默认实现。
 * <p>
 * 由 MobileAppBundleController 调用，委托 {@link MobileAppBundleService} 落库并写审计日志。
 *
 * @see JnksIotMobileAppBundleService
 */
@Service
@AllArgsConstructor
public class DefaultJnksIotMobileAppBundleService extends AbstractJnksIotEntityService implements JnksIotMobileAppBundleService {

    private final MobileAppBundleService mobileAppBundleService;

    /** 保存移动应用包并可绑定 OAuth2 客户端，写审计。 */
    @Override
    public MobileAppBundle save(MobileAppBundle mobileAppBundle, List<OAuth2ClientId> oauth2Clients, User user) throws Exception {
        ActionType actionType = mobileAppBundle.getId() == null ? ActionType.ADDED : ActionType.UPDATED;
        TenantId tenantId = mobileAppBundle.getTenantId();
        try {
            MobileAppBundle savedMobileAppBundle = checkNotNull(mobileAppBundleService.saveMobileAppBundle(tenantId, mobileAppBundle));
            if (CollectionUtils.isNotEmpty(oauth2Clients)) {
                mobileAppBundleService.updateOauth2Clients(tenantId, savedMobileAppBundle.getId(), oauth2Clients);
            }
            logEntityActionService.logEntityAction(tenantId, savedMobileAppBundle.getId(), savedMobileAppBundle, actionType, user);
            return savedMobileAppBundle;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.MOBILE_APP), mobileAppBundle, actionType, user, e);
            throw e;
        }
    }

    /** 更新绑定的 OAuth2 客户端并写 UPDATED 审计。 */
    @Override
    public void updateOauth2Clients(MobileAppBundle mobileAppBundle, List<OAuth2ClientId> oAuth2ClientIds, User user) {
        ActionType actionType = ActionType.UPDATED;
        TenantId tenantId = mobileAppBundle.getTenantId();
        MobileAppBundleId mobileAppBundleId = mobileAppBundle.getId();
        try {
            mobileAppBundleService.updateOauth2Clients(tenantId, mobileAppBundleId, oAuth2ClientIds);
            logEntityActionService.logEntityAction(tenantId, mobileAppBundleId, mobileAppBundle, actionType, user, oAuth2ClientIds);
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, mobileAppBundleId, mobileAppBundle, actionType, user, e, oAuth2ClientIds);
            throw e;
        }
    }

    /** 删除移动应用包并写 DELETED 审计。 */
    @Override
    public void delete(MobileAppBundle mobileAppBundle, User user) {
        ActionType actionType = ActionType.DELETED;
        TenantId tenantId = mobileAppBundle.getTenantId();
        MobileAppBundleId mobileAppBundleId = mobileAppBundle.getId();
        try {
            mobileAppBundleService.deleteMobileAppBundleById(tenantId, mobileAppBundleId);
            logEntityActionService.logEntityAction(tenantId, mobileAppBundleId, mobileAppBundle, actionType, user);
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, mobileAppBundleId, mobileAppBundle, actionType, user, e);
            throw e;
        }
    }
}
