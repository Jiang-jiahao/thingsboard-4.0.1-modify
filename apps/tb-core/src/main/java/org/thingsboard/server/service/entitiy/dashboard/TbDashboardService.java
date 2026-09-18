package org.thingsboard.server.service.entitiy.dashboard;

import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.Dashboard;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.service.entitiy.SimpleTbEntityService;

import java.util.Set;

/**
 * 仪表板业务层契约：CRUD 以及分配到客户 / 公开客户。
 * <p>
 * 由 DashboardController 调用；实现类委托 Dashboard DAO，并写审计日志。
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

    /** 取消仪表板与指定客户的分配。 */
    Dashboard unassignDashboardFromCustomer(Dashboard dashboard, Customer customer, User user) throws ThingsboardException;

}
