package org.thingsboard.server.service.entitiy.asset;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.dao.asset.AssetService;
import org.thingsboard.server.service.entitiy.AbstractTbEntityService;

/**
 * {@link TbAssetService} 的默认实现。
 * <p>
 * 由 AssetController 调用，委托 {@link AssetService} 落库；写审计日志，保存时尝试 autoCommit。
 * 分配到客户会经 {@code logEntityActionService} 触发规则引擎。
 *
 * @see TbAssetService
 */
@Service
@AllArgsConstructor
public class DefaultTbAssetService extends AbstractTbEntityService implements TbAssetService {

    private final AssetService assetService;

    /** 保存资产，写审计并尝试 autoCommit。 */
    @Override
    public Asset save(Asset asset, User user) throws Exception {
        ActionType actionType = asset.getId() == null ? ActionType.ADDED : ActionType.UPDATED;
        TenantId tenantId = asset.getTenantId();
        try {
            Asset savedAsset = checkNotNull(assetService.saveAsset(asset));
            autoCommit(user, savedAsset.getId());
            logEntityActionService.logEntityAction(tenantId, savedAsset.getId(), savedAsset, asset.getCustomerId(),
                    actionType, user);
            return savedAsset;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.ASSET), asset, actionType, user, e);
            throw e;
        }
    }

    /** 删除资产并写 DELETED 审计。 */
    @Override
    @Transactional
    public void delete(Asset asset, User user) {
        ActionType actionType = ActionType.DELETED;
        TenantId tenantId = asset.getTenantId();
        AssetId assetId = asset.getId();
        try {
            assetService.deleteAsset(tenantId, assetId);
            logEntityActionService.logEntityAction(tenantId, assetId, asset, asset.getCustomerId(), actionType, user, assetId.toString());
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.ASSET), actionType, user, e,
                    assetId.toString());
            throw e;
        }
    }

    /** 将资产分配给客户并写审计。 */
    @Override
    public Asset assignAssetToCustomer(TenantId tenantId, AssetId assetId, Customer customer, User user) throws ThingsboardException {
        ActionType actionType = ActionType.ASSIGNED_TO_CUSTOMER;
        CustomerId customerId = customer.getId();
        try {
            Asset savedAsset = checkNotNull(assetService.assignAssetToCustomer(tenantId, assetId, customerId));
            logEntityActionService.logEntityAction(tenantId, assetId, savedAsset, customerId, actionType, user,
                    assetId.toString(), customerId.toString(), customer.getName());

            return savedAsset;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.ASSET), actionType, user, e,
                    assetId.toString(), customerId.toString());
            throw e;
        }
    }

    /** 取消资产与客户的分配并写审计。 */
    @Override
    public Asset unassignAssetToCustomer(TenantId tenantId, AssetId assetId, Customer customer, User user) throws ThingsboardException {
        ActionType actionType = ActionType.UNASSIGNED_FROM_CUSTOMER;
        try {
            Asset savedAsset = checkNotNull(assetService.unassignAssetFromCustomer(tenantId, assetId));
            CustomerId customerId = customer.getId();
            logEntityActionService.logEntityAction(tenantId, assetId, savedAsset, customerId, actionType, user,
                    assetId.toString(), customerId.toString(), customer.getName());

            return savedAsset;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.ASSET), actionType, user, e, assetId.toString());
            throw e;
        }
    }

    /** 将资产分配给公开客户并写审计。 */
    @Override
    public Asset assignAssetToPublicCustomer(TenantId tenantId, AssetId assetId, User user) throws ThingsboardException {
        ActionType actionType = ActionType.ASSIGNED_TO_CUSTOMER;
        try {
            Customer publicCustomer = customerService.findOrCreatePublicCustomer(tenantId);
            Asset savedAsset = checkNotNull(assetService.assignAssetToCustomer(tenantId, assetId, publicCustomer.getId()));
            CustomerId customerId = publicCustomer.getId();
            logEntityActionService.logEntityAction(tenantId, assetId, savedAsset, customerId, actionType, user,
                    assetId.toString(), customerId.toString(), publicCustomer.getName());

            return savedAsset;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.ASSET), actionType, user, e, assetId.toString());
            throw e;
        }
    }
}
