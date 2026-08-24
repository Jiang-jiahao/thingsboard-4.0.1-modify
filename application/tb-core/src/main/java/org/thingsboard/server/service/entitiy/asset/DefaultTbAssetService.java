/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.entitiy.asset;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.EdgeId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.dao.asset.AssetService;
import org.thingsboard.server.service.entitiy.AbstractTbEntityService;

/**
 * {@link TbAssetService} 的默认实现。
 * <p>
 * 由 AssetController 调用，委托 {@link AssetService} 落库；写审计日志，保存时尝试 autoCommit。
 * 分配到客户/Edge 会经 {@code logEntityActionService} 触发规则引擎与 Edge 同步。
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

    /** 将资产分配给 Edge 并写审计。 */
    @Override
    public Asset assignAssetToEdge(TenantId tenantId, AssetId assetId, Edge edge, User user) throws ThingsboardException {
        ActionType actionType = ActionType.ASSIGNED_TO_EDGE;
        EdgeId edgeId = edge.getId();
        try {
            Asset savedAsset = checkNotNull(assetService.assignAssetToEdge(tenantId, assetId, edgeId));
            logEntityActionService.logEntityAction(tenantId, assetId, savedAsset, savedAsset.getCustomerId(),
                    actionType, user, assetId.toString(), edgeId.toString(), edge.getName());
            return savedAsset;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.ASSET), actionType,
                    user, e, assetId.toString(), edgeId.toString());
            throw e;
        }
    }

    /** 取消资产与 Edge 的分配并写审计。 */
    @Override
    public Asset unassignAssetFromEdge(TenantId tenantId, Asset asset, Edge edge, User user) throws ThingsboardException {
        ActionType actionType = ActionType.UNASSIGNED_FROM_EDGE;
        AssetId assetId = asset.getId();
        EdgeId edgeId = edge.getId();
        try {
            Asset savedAsset = checkNotNull(assetService.unassignAssetFromEdge(tenantId, assetId, edgeId));
            logEntityActionService.logEntityAction(tenantId, assetId, asset, asset.getCustomerId(),
                    actionType, user, assetId.toString(), edgeId.toString(), edge.getName());

            return savedAsset;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.ASSET), actionType,
                    user, e, assetId.toString(), edgeId.toString());
            throw e;
        }
    }
}
