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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.ResourceType;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.dao.resource.ResourceService;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.install.InstallScripts;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
@TbCoreComponent
@RequiredArgsConstructor
@Slf4j
public class GatewaysDashboardResourceLoader {

    private static final String GATEWAYS_DASHBOARD_KEY = "gateways_dashboard.json";

    private final ResourceService resourceService;
    private final PartitionService partitionService;
    private final InstallScripts installScripts;

    @Value("${transport.gateway.dashboard.resource.update-on-startup:true}")
    private boolean updateOnStartup;

    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void loadGatewaysDashboardResource() {
        if (!updateOnStartup) {
            return;
        }
        if (!partitionService.isMyPartition(ServiceType.TB_CORE, TenantId.SYS_TENANT_ID, TenantId.SYS_TENANT_ID)) {
            return;
        }
        Path dashboardPath = Paths.get(installScripts.getDataDir(), InstallScripts.RESOURCES_DIR,
                InstallScripts.DASHBOARDS_DIR, GATEWAYS_DASHBOARD_KEY);
        if (!Files.isRegularFile(dashboardPath)) {
            log.warn("Gateways dashboard file not found at {}, skip system resource update", dashboardPath);
            return;
        }
        try {
            byte[] data = Files.readAllBytes(dashboardPath);
            resourceService.createOrUpdateSystemResource(ResourceType.DASHBOARD, null, GATEWAYS_DASHBOARD_KEY, data);
            log.info("Updated system gateways dashboard resource from {}", dashboardPath);
        } catch (Exception e) {
            log.error("Failed to update gateways dashboard system resource from {}", dashboardPath, e);
        }
    }

}
