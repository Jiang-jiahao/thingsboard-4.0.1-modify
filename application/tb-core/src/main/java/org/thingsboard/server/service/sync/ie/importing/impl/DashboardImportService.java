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
package org.thingsboard.server.service.sync.ie.importing.impl;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.Dashboard;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.ShortCustomerInfo;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DashboardId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.sync.ie.EntityExportData;
import org.thingsboard.server.dao.dashboard.DashboardService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.sync.vc.data.EntitiesImportCtx;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 针对 {@link Dashboard} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 递归把别名和 widget 动作中的 UUID 映射为内部 ID；按标题兜底匹配已有仪表板；
 * 保存时同步客户分配（assign/unassign）；配置 JSON 变化也会触发更新。
 */
@Service
@TbCoreComponent
@RequiredArgsConstructor
public class DashboardImportService extends BaseEntityImportService<DashboardId, Dashboard, EntityExportData<Dashboard>> {

    private static final LinkedHashSet<EntityType> HINTS = new LinkedHashSet<>(Arrays.asList(EntityType.DASHBOARD, EntityType.DEVICE, EntityType.ASSET));
    public static final Pattern WIDGET_CONFIG_PROCESSED_FIELDS_PATTERN = Pattern.compile(".*Id.*");

    private final DashboardService dashboardService;

    @Override
    protected void setOwner(TenantId tenantId, Dashboard dashboard, IdProvider idProvider) {
        dashboard.setTenantId(tenantId);
    }

    /**
     * 基类匹配失败且允许按名称查找时，再用标题在租户下查找已有仪表板。
     */
    @Override
    protected Dashboard findExistingEntity(EntitiesImportCtx ctx, Dashboard dashboard, IdProvider idProvider) {
        Dashboard existingDashboard = super.findExistingEntity(ctx, dashboard, idProvider);
        if (existingDashboard == null && ctx.isFindExistingByName()) {
            existingDashboard = dashboardService.findTenantDashboardsByTitle(ctx.getTenantId(), dashboard.getName()).stream().findFirst().orElse(null);
        }
        return existingDashboard;
    }

    /**
     * 将实体别名与 widget 动作配置中的 UUID 替换为当前环境内部 ID。
     */
    @Override
    protected Dashboard prepare(EntitiesImportCtx ctx, Dashboard dashboard, Dashboard old, EntityExportData<Dashboard> exportData, IdProvider idProvider) {
        for (JsonNode entityAlias : dashboard.getEntityAliasesConfig()) {
            replaceIdsRecursively(ctx, idProvider, entityAlias, Set.of("id"), null, HINTS);
        }
        for (JsonNode widgetConfig : dashboard.getWidgetsConfig()) {
            replaceIdsRecursively(ctx, idProvider, JacksonUtil.getSafely(widgetConfig, "config", "actions"), Collections.emptySet(), WIDGET_CONFIG_PROCESSED_FIELDS_PATTERN, HINTS);
        }
        return dashboard;
    }

    /**
     * 新建时分配客户；更新时对客户做差量 assign/unassign 后再保存。
     */
    @Override
    protected Dashboard saveOrUpdate(EntitiesImportCtx ctx, Dashboard dashboard, EntityExportData<Dashboard> exportData, IdProvider idProvider) {
        var tenantId = ctx.getTenantId();

        Set<ShortCustomerInfo> assignedCustomers = Optional.ofNullable(dashboard.getAssignedCustomers()).orElse(Collections.emptySet()).stream()
                .peek(customerInfo -> customerInfo.setCustomerId(idProvider.getInternalId(customerInfo.getCustomerId())))
                .collect(Collectors.toSet());

        if (dashboard.getId() == null) {
            dashboard.setAssignedCustomers(assignedCustomers);
            dashboard = dashboardService.saveDashboard(dashboard);
            for (ShortCustomerInfo customerInfo : assignedCustomers) {
                dashboard = dashboardService.assignDashboardToCustomer(tenantId, dashboard.getId(), customerInfo.getCustomerId());
            }
        } else {
            Set<CustomerId> existingAssignedCustomers = Optional.ofNullable(dashboardService.findDashboardById(tenantId, dashboard.getId()).getAssignedCustomers())
                    .orElse(Collections.emptySet()).stream().map(ShortCustomerInfo::getCustomerId).collect(Collectors.toSet());
            Set<CustomerId> newAssignedCustomers = assignedCustomers.stream().map(ShortCustomerInfo::getCustomerId).collect(Collectors.toSet());

            Set<CustomerId> toUnassign = new HashSet<>(existingAssignedCustomers);
            toUnassign.removeAll(newAssignedCustomers);
            for (CustomerId customerId : toUnassign) {
                assignedCustomers = dashboardService.unassignDashboardFromCustomer(tenantId, dashboard.getId(), customerId).getAssignedCustomers();
            }

            Set<CustomerId> toAssign = new HashSet<>(newAssignedCustomers);
            toAssign.removeAll(existingAssignedCustomers);
            for (CustomerId customerId : toAssign) {
                assignedCustomers = dashboardService.assignDashboardToCustomer(tenantId, dashboard.getId(), customerId).getAssignedCustomers();
            }
            dashboard.setAssignedCustomers(assignedCustomers);
            dashboard = dashboardService.saveDashboard(dashboard);
        }
        return dashboard;
    }

    @Override
    protected Dashboard deepCopy(Dashboard dashboard) {
        return new Dashboard(dashboard);
    }

    /** 实体字段相同但 configuration JSON 不同时仍视为需要更新。 */
    @Override
    protected boolean compare(EntitiesImportCtx ctx, EntityExportData<Dashboard> exportData, Dashboard prepared, Dashboard existing) {
        return super.compare(ctx, exportData, prepared, existing) || !prepared.getConfiguration().equals(existing.getConfiguration());
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.DASHBOARD;
    }

}
