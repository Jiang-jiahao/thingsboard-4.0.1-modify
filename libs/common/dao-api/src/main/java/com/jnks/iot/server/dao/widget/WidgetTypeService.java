package com.jnks.iot.server.dao.widget;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.WidgetTypeId;
import com.jnks.iot.server.common.data.id.WidgetsBundleId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.widget.DeprecatedFilter;
import com.jnks.iot.server.common.data.widget.WidgetType;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;
import com.jnks.iot.server.common.data.widget.WidgetTypeFilter;
import com.jnks.iot.server.common.data.widget.WidgetTypeInfo;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.List;

public interface WidgetTypeService extends EntityDaoService {

    WidgetType findWidgetTypeById(TenantId tenantId, WidgetTypeId widgetTypeId);

    WidgetTypeDetails findWidgetTypeDetailsById(TenantId tenantId, WidgetTypeId widgetTypeId);

    WidgetTypeInfo findWidgetTypeInfoById(TenantId tenantId, WidgetTypeId widgetTypeId);

    boolean widgetTypeExistsByTenantIdAndWidgetTypeId(TenantId tenantId, WidgetTypeId widgetTypeId);

    WidgetTypeDetails saveWidgetType(WidgetTypeDetails widgetType);

    void deleteWidgetType(TenantId tenantId, WidgetTypeId widgetTypeId);

    PageData<WidgetTypeInfo> findSystemWidgetTypesByPageLink(WidgetTypeFilter widgetTypeFilter, PageLink pageLink);

    PageData<WidgetTypeInfo> findAllTenantWidgetTypesByTenantIdAndPageLink(WidgetTypeFilter widgetTypeFilter, PageLink pageLink);

    PageData<WidgetTypeInfo> findTenantWidgetTypesByTenantIdAndPageLink(WidgetTypeFilter widgetTypeFilter, PageLink pageLink);

    List<WidgetType> findWidgetTypesByWidgetsBundleId(TenantId tenantId, WidgetsBundleId widgetsBundleId);

    List<WidgetTypeDetails> findWidgetTypesDetailsByWidgetsBundleId(TenantId tenantId, WidgetsBundleId widgetsBundleId);

    PageData<WidgetTypeInfo> findWidgetTypesInfosByWidgetsBundleId(TenantId tenantId, WidgetsBundleId widgetsBundleId, boolean fullSearch, DeprecatedFilter deprecatedFilter, List<String> widgetTypes, PageLink pageLink);

    List<String> findWidgetFqnsByWidgetsBundleId(TenantId tenantId, WidgetsBundleId widgetsBundleId);

    WidgetType findWidgetTypeByTenantIdAndFqn(TenantId tenantId, String fqn);

    WidgetTypeDetails findWidgetTypeDetailsByTenantIdAndFqn(TenantId tenantId, String fqn);

    void updateWidgetsBundleWidgetTypes(TenantId tenantId, WidgetsBundleId widgetsBundleId, List<WidgetTypeId> widgetTypeIds);

    void updateWidgetsBundleWidgetFqns(TenantId tenantId, WidgetsBundleId widgetsBundleId, List<String> widgetFqns);

    void deleteWidgetTypesByTenantId(TenantId tenantId);

    void deleteWidgetTypesByBundleId(TenantId tenantId, WidgetsBundleId bundleId);

    PageData<WidgetTypeId> findAllWidgetTypesIds(PageLink pageLink);

}
