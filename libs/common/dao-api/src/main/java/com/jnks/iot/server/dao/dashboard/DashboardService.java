package com.jnks.iot.server.dao.dashboard;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.DashboardInfo;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.List;

public interface DashboardService extends EntityDaoService {

    Dashboard findDashboardById(TenantId tenantId, DashboardId dashboardId);

    ListenableFuture<Dashboard> findDashboardByIdAsync(TenantId tenantId, DashboardId dashboardId);

    DashboardInfo findDashboardInfoById(TenantId tenantId, DashboardId dashboardId);

    String findDashboardTitleById(TenantId tenantId, DashboardId dashboardId);

    ListenableFuture<DashboardInfo> findDashboardInfoByIdAsync(TenantId tenantId, DashboardId dashboardId);

    Dashboard saveDashboard(Dashboard dashboard, boolean doValidate);

    Dashboard saveDashboard(Dashboard dashboard);

    Dashboard assignDashboardToCustomer(TenantId tenantId, DashboardId dashboardId, CustomerId customerId);

    Dashboard unassignDashboardFromCustomer(TenantId tenantId, DashboardId dashboardId, CustomerId customerId);

    void deleteDashboard(TenantId tenantId, DashboardId dashboardId);

    PageData<DashboardInfo> findDashboardsByTenantId(TenantId tenantId, PageLink pageLink);

    PageData<DashboardInfo> findMobileDashboardsByTenantId(TenantId tenantId, PageLink pageLink);

    void deleteDashboardsByTenantId(TenantId tenantId);

    PageData<DashboardInfo> findDashboardsByTenantIdAndCustomerId(TenantId tenantId, CustomerId customerId, PageLink pageLink);

    PageData<DashboardInfo> findMobileDashboardsByTenantIdAndCustomerId(TenantId tenantId, CustomerId customerId, PageLink pageLink);

    void unassignCustomerDashboards(TenantId tenantId, CustomerId customerId);

    void updateCustomerDashboards(TenantId tenantId, CustomerId customerId);

    DashboardInfo findFirstDashboardInfoByTenantIdAndName(TenantId tenantId, String name);

    ListenableFuture<DashboardInfo> findFirstDashboardInfoByTenantIdAndNameAsync(TenantId tenantId, String name);

    List<Dashboard> findTenantDashboardsByTitle(TenantId tenantId, String title);

    boolean existsById(TenantId tenantId, DashboardId dashboardId);

    PageData<DashboardId> findAllDashboardsIds(PageLink pageLink);

}
