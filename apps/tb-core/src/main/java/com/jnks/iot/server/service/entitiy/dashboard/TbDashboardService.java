package com.jnks.iot.server.service.entitiy.dashboard;

import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.service.entitiy.SimpleTbEntityService;

import java.util.Set;

/**
 * 仪表板业务层契约：CRUD 以及分配到客户 / 公开客户。
 * <p>
 * 由 DashboardController 调用；实现类委托 Dashboard DAO，并写审计日志。
 */
public interface TbDashboardService extends SimpleTbEntityService<Dashboard> {

    /** 将仪表板分配给客户。 */
    Dashboard assignDashboardToCustomer(Dashboard dashboard, Customer customer, User user) throws JnksIotException;

    /** 将仪表板分配给公开客户。 */
    Dashboard assignDashboardToPublicCustomer(Dashboard dashboard, User user) throws JnksIotException;

    /** 取消仪表板与公开客户的分配。 */
    Dashboard unassignDashboardFromPublicCustomer(Dashboard dashboard, User user) throws JnksIotException;

    /** 用给定客户集合整体替换仪表板的客户分配。 */
    Dashboard updateDashboardCustomers(Dashboard dashboard, Set<CustomerId> customerIds, User user) throws JnksIotException;

    /** 为仪表板追加客户分配。 */
    Dashboard addDashboardCustomers(Dashboard dashboard, Set<CustomerId> customerIds, User user) throws JnksIotException;

    /** 从仪表板移除若干客户分配。 */
    Dashboard removeDashboardCustomers(Dashboard dashboard, Set<CustomerId> customerIds, User user) throws JnksIotException;

    /** 取消仪表板与指定客户的分配。 */
    Dashboard unassignDashboardFromCustomer(Dashboard dashboard, Customer customer, User user) throws JnksIotException;

}
