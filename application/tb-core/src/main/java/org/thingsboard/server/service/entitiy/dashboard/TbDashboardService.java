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
package org.thingsboard.server.service.entitiy.dashboard;

import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.Dashboard;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.edge.Edge;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DashboardId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.service.entitiy.SimpleTbEntityService;

import java.util.Set;

/**
 * 仪表板业务层契约：CRUD 以及分配到客户 / 公开客户 / Edge。
 * <p>
 * 由 DashboardController 调用；实现类委托 Dashboard DAO，写审计日志并触发 Edge 同步。
 */
public interface TbDashboardService extends SimpleTbEntityService<Dashboard> {

    /** 将仪表板分配给客户。 */
    Dashboard assignDashboardToCustomer(Dashboard dashboard, Customer customer, User user) throws ThingsboardException;

    /** 将仪表板分配给公开客户。 */
    Dashboard assignDashboardToPublicCustomer(Dashboard dashboard, User user) throws ThingsboardException;

    /** 取消仪表板与公开客户的分配。 */
    Dashboard unassignDashboardFromPublicCustomer(Dashboard dashboard, User user) throws ThingsboardException;

    /** 用给定客户集合整体替换仪表板的客户分配。 */
    Dashboard updateDashboardCustomers(Dashboard dashboard, Set<CustomerId> customerIds, User user) throws ThingsboardException;

    /** 为仪表板追加客户分配。 */
    Dashboard addDashboardCustomers(Dashboard dashboard, Set<CustomerId> customerIds, User user) throws ThingsboardException;

    /** 从仪表板移除若干客户分配。 */
    Dashboard removeDashboardCustomers(Dashboard dashboard, Set<CustomerId> customerIds, User user) throws ThingsboardException;

    /** 将仪表板分配给 Edge。 */
    Dashboard asignDashboardToEdge(TenantId tenantId, DashboardId dashboardId, Edge edge, User user) throws ThingsboardException;

    /** 取消仪表板与 Edge 的分配。 */
    Dashboard unassignDashboardFromEdge(Dashboard dashboard, Edge edge, User user) throws ThingsboardException;

    /** 取消仪表板与指定客户的分配。 */
    Dashboard unassignDashboardFromCustomer(Dashboard dashboard, Customer customer, User user) throws ThingsboardException;

}
