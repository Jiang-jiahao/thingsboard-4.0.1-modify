package com.jnks.iot.server.dao.dashboard;

import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.ExportableEntityDao;
import com.jnks.iot.server.dao.TenantEntityDao;

import java.util.List;
import java.util.UUID;

/**
 * The Interface DashboardDao.
 */
public interface DashboardDao extends Dao<Dashboard>, TenantEntityDao<Dashboard>, ExportableEntityDao<DashboardId, Dashboard> {

    /**
     * Save or update dashboard object
     *
     * @param dashboard the dashboard object
     * @return saved dashboard object
     */
    Dashboard save(TenantId tenantId, Dashboard dashboard);

    List<Dashboard> findByTenantIdAndTitle(UUID tenantId, String title);

    PageData<DashboardId> findIdsByTenantId(TenantId tenantId, PageLink pageLink);

    PageData<DashboardId> findAllIds(PageLink pageLink);

}
