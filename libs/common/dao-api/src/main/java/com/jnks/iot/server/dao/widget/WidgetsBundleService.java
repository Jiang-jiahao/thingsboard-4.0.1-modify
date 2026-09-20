package com.jnks.iot.server.dao.widget;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.WidgetsBundleId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.widget.WidgetsBundle;
import com.jnks.iot.server.common.data.widget.WidgetsBundleFilter;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.List;
import java.util.stream.Stream;

public interface WidgetsBundleService extends EntityDaoService {

    WidgetsBundle findWidgetsBundleById(TenantId tenantId, WidgetsBundleId widgetsBundleId);

    WidgetsBundle saveWidgetsBundle(WidgetsBundle widgetsBundle);

    void deleteWidgetsBundle(TenantId tenantId, WidgetsBundleId widgetsBundleId);

    WidgetsBundle findWidgetsBundleByTenantIdAndAlias(TenantId tenantId, String alias);

    PageData<WidgetsBundle> findSystemWidgetsBundlesByPageLink(WidgetsBundleFilter widgetsBundleFilter, PageLink pageLink);

    List<WidgetsBundle> findSystemWidgetsBundles(TenantId tenantId);

    PageData<WidgetsBundle> findTenantWidgetsBundlesByTenantId(TenantId tenantId, PageLink pageLink);

    PageData<WidgetsBundle> findAllTenantWidgetsBundlesByTenantIdAndPageLink(WidgetsBundleFilter widgetsBundleFilter, PageLink pageLink);

    PageData<WidgetsBundle> findTenantWidgetsBundlesByTenantIdAndPageLink(WidgetsBundleFilter widgetsBundleFilter, PageLink pageLink);

    List<WidgetsBundle> findAllTenantWidgetsBundlesByTenantId(TenantId tenantId);

    void deleteWidgetsBundlesByTenantId(TenantId tenantId);

    void updateSystemWidgets(Stream<String> bundles, Stream<String> widgets);

}
