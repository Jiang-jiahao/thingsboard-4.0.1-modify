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

import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.TenantId;

/**
 * 资产业务层契约：CRUD 以及分配到客户 / 公开客户 / Edge。
 * <p>
 * 由 AssetController 调用；实现类委托 Asset DAO，并写审计日志与 Edge 同步事件。
 */
public interface TbAssetService {

    /** 保存资产。 */
    Asset save(Asset asset, User user) throws Exception;

    /** 删除资产。 */
    void delete(Asset asset, User user);

    /** 将资产分配给客户。 */
    Asset assignAssetToCustomer(TenantId tenantId, AssetId assetId, Customer customer, User user) throws ThingsboardException;

    /** 取消资产与客户的分配。 */
    Asset unassignAssetToCustomer(TenantId tenantId, AssetId assetId, Customer customer, User user) throws ThingsboardException;

    /** 将资产分配给公开客户。 */
    Asset assignAssetToPublicCustomer(TenantId tenantId, AssetId assetId, User user) throws ThingsboardException;

    /** 将资产分配给 Edge。 */
    Asset assignAssetToEdge(TenantId tenantId, AssetId assetId, Edge edge, User user) throws ThingsboardException;

    /** 取消资产与 Edge 的分配。 */
    Asset unassignAssetFromEdge(TenantId tenantId, Asset asset, Edge edge, User user) throws ThingsboardException;

}
