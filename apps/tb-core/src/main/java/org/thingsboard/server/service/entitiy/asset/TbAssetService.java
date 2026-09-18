package org.thingsboard.server.service.entitiy.asset;

import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.TenantId;

/**
 * 资产业务层契约：CRUD 以及分配到客户 / 公开客户。
 * <p>
 * 由 AssetController 调用；实现类委托 Asset DAO，并写审计日志。
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

}
