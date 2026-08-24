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
package org.thingsboard.server.service.entitiy.device;

import com.google.common.util.concurrent.ListenableFuture;
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.dao.device.claim.ClaimResult;
import org.thingsboard.server.dao.device.claim.ReclaimResult;

/**
 * 设备业务层契约：CRUD、凭据、认领，以及分配到客户/租户/Edge。
 * <p>
 * 由 DeviceController 等调用；实现类委托 Device DAO，并写审计日志、版本控制提交与 Edge 同步事件。
 */
public interface TbDeviceService {

    /** 保存设备并可选设置 access token。 */
    Device save(Device device, String accessToken, User user) throws Exception;

    /** 保存设备及其凭据。 */
    Device saveDeviceWithCredentials(Device device, DeviceCredentials deviceCredentials, User user) throws ThingsboardException;

    /** 删除设备。 */
    void delete(Device device, User user);

    /** 将设备分配给客户。 */
    Device assignDeviceToCustomer(TenantId tenantId, DeviceId deviceId, Customer customer, User user) throws ThingsboardException;

    /** 取消设备与客户的分配。 */
    Device unassignDeviceFromCustomer(Device device, Customer customer, User user) throws ThingsboardException;

    /** 将设备分配给公开客户。 */
    Device assignDeviceToPublicCustomer(TenantId tenantId, DeviceId deviceId, User user) throws ThingsboardException;

    /** 读取设备凭据（会记 CREDENTIALS_READ 审计）。 */
    DeviceCredentials getDeviceCredentialsByDeviceId(Device device, User user) throws ThingsboardException;

    /** 更新设备凭据。 */
    DeviceCredentials updateDeviceCredentials(Device device, DeviceCredentials deviceCredentials, User user) throws ThingsboardException;

    /** 客户认领设备。 */
    ListenableFuture<ClaimResult> claimDevice(TenantId tenantId, Device device, CustomerId customerId, String secretKey, User user);

    /** 回收已认领设备。 */
    ListenableFuture<ReclaimResult> reclaimDevice(TenantId tenantId, Device device, User user);

    /** 将设备转移到另一租户。 */
    Device assignDeviceToTenant(Device device, Tenant newTenant, User user);

    /** 将设备分配给 Edge。 */
    Device assignDeviceToEdge(TenantId tenantId, DeviceId deviceId, Edge edge, User user) throws ThingsboardException;

    /** 取消设备与 Edge 的分配。 */
    Device unassignDeviceFromEdge(Device device, Edge edge, User user) throws ThingsboardException;
}
