package com.jnks.iot.server.dao.dashboard;

import com.jnks.iot.server.common.data.DashboardInfo;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.ImageContainerDao;
import com.jnks.iot.server.dao.ResourceContainerDao;

import java.util.UUID;

/**
 * The Interface DashboardInfoDao.
 */
public interface DashboardInfoDao extends Dao<DashboardInfo>, ImageContainerDao<DashboardInfo>, ResourceContainerDao<DashboardInfo> {

    /**
     * Find dashboards by tenantId and page link.
     *
     * @param tenantId the tenantId
     * @param pageLink the page link
     * @return the list of dashboard objects
     */
    PageData<DashboardInfo> findDashboardsByTenantId(UUID tenantId, PageLink pageLink);

    /**
     * Find dashboards not hidden for mobile by tenantId and page link.
     *
     * @param tenantId the tenantId
     * @param pageLink the page link
     * @return the list of dashboard objects
     */
    PageData<DashboardInfo> findMobileDashboardsByTenantId(UUID tenantId, PageLink pageLink);

    /**
     * Find dashboards by tenantId, customerId and page link.
     *
     * @param tenantId the tenantId
     * @param customerId the customerId
     * @param pageLink the page link
     * @return the list of dashboard objects
     */
    PageData<DashboardInfo> findDashboardsByTenantIdAndCustomerId(UUID tenantId, UUID customerId, PageLink pageLink);

    /**
     * Find dashboards not hidden for mobile by tenantId, customerId and page link.
     *
     * @param tenantId the tenantId
     * @param customerId the customerId
     * @param pageLink the page link
     * @return the list of dashboard objects
     */
    PageData<DashboardInfo> findMobileDashboardsByTenantIdAndCustomerId(UUID tenantId, UUID customerId, PageLink pageLink);

    DashboardInfo findFirstByTenantIdAndName(UUID tenantId, String name);

    String findTitleById(UUID tenantId, UUID dashboardId);

}
