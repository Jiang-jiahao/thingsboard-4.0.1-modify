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
package org.thingsboard.server.service.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.info.BuildProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.server.common.data.EdgeUpgradeMessage;
import org.thingsboard.server.common.data.UpdateMessage;
import org.thingsboard.server.common.data.notification.rule.trigger.NewPlatformVersionTrigger;
import org.thingsboard.server.common.msg.notification.NotificationRuleProcessor;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.edge.instructions.EdgeInstallInstructionsService;
import org.thingsboard.server.service.edge.instructions.EdgeUpgradeInstructionsService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 平台更新检查服务：定时向 ThingsBoard 更新服务器查询新版本，并刷新 Edge 安装/升级说明。
 * <p>
 * <b>职责：</b>上报 instanceId/platform/version；发现新版本时触发
 * {@code NewPlatformVersion} 通知；同步 Edge 安装版本与升级映射。
 * <p>
 * <b>触发方式：</b>启动后每小时一次（{@code updates.enabled=true}）。
 * <p>
 * <b>通知对象：</b>配置了新版本规则的通知目标。
 */
@Service
@TbCoreComponent
@Slf4j
public class DefaultUpdateService implements UpdateService {

    private static final String INSTANCE_ID_FILE = ".instance_id";
    private static final String UPDATE_SERVER_BASE_URL = "https://updates.thingsboard.io";

    private static final String PLATFORM_PARAM = "platform";
    private static final String VERSION_PARAM = "version";
    private static final String INSTANCE_ID_PARAM = "instanceId";

    @Value("${updates.enabled}")
    private boolean updatesEnabled;

    @Autowired(required = false)
    private BuildProperties buildProperties;

    @Autowired
    private NotificationRuleProcessor notificationRuleProcessor;

    @Autowired(required = false)
    private EdgeInstallInstructionsService edgeInstallInstructionsService;

    @Autowired(required = false)
    private EdgeUpgradeInstructionsService edgeUpgradeInstructionsService;

    private final ScheduledExecutorService scheduler = ThingsBoardExecutors.newSingleThreadScheduledExecutor("tb-update-service");

    private ScheduledFuture<?> checkUpdatesFuture = null;
    private final RestTemplate restClient = new RestTemplate();

    private UpdateMessage updateMessage;

    private String platform;
    private String version;
    private UUID instanceId = null;

    /** 启动后开始定时检查更新。 */
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void init() {
        version = buildProperties != null ? buildProperties.getVersion() : "unknown";
        updateMessage = new UpdateMessage(false, version, "", "",
                "https://thingsboard.io/docs/reference/releases",
                "https://thingsboard.io/docs/reference/releases");
        if (updatesEnabled) {
            try {
                platform = System.getProperty("platform", "unknown");
                instanceId = parseInstanceId();
                checkUpdatesFuture = scheduler.scheduleAtFixedRate(checkUpdatesRunnable, 0, 1, TimeUnit.HOURS);
            } catch (Exception e) {
                //Do nothing
            }
        }
    }

    private UUID parseInstanceId() throws IOException {
        UUID result = null;
        Path instanceIdPath = Paths.get(INSTANCE_ID_FILE);
        if (instanceIdPath.toFile().exists()) {
            byte[] data = Files.readAllBytes(instanceIdPath);
            if (data.length > 0) {
                try {
                    result = UUID.fromString(new String(data));
                } catch (IllegalArgumentException e) {
                    //Do nothing
                }
            }
        }
        if (result == null) {
            result = UUID.randomUUID();
            Files.write(instanceIdPath, result.toString().getBytes());
        }
        return result;
    }

    @PreDestroy
    private void destroy() {
        try {
            if (checkUpdatesFuture != null) {
                checkUpdatesFuture.cancel(true);
            }
            scheduler.shutdownNow();
        } catch (Exception e) {
            //Do nothing
        }
    }

    Runnable checkUpdatesRunnable = () -> {
        try {
            log.trace("Executing check update method for instanceId [{}], platform [{}] and version [{}]", instanceId, platform, version);
            var headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            ObjectNode request = JacksonUtil.newObjectNode();
            request.put(PLATFORM_PARAM, platform);
            request.put(VERSION_PARAM, version);
            request.put(INSTANCE_ID_PARAM, instanceId.toString());
            UpdateMessage prevUpdateMessage = updateMessage;
            updateMessage = restClient.postForObject(UPDATE_SERVER_BASE_URL + "/api/v2/thingsboard/updates", new HttpEntity<>(request.toString(), headers), UpdateMessage.class);
            if (updateMessage != null && updateMessage.isUpdateAvailable() && !updateMessage.equals(prevUpdateMessage)) {
                notificationRuleProcessor.process(NewPlatformVersionTrigger.builder()
                        .updateInfo(updateMessage)
                        .build());
            }
            ObjectNode edgeRequest = JacksonUtil.newObjectNode().put(VERSION_PARAM, version);
            String edgeInstallVersion = restClient.postForObject(UPDATE_SERVER_BASE_URL + "/api/v1/edge/installMapping", new HttpEntity<>(edgeRequest.toString(), headers), String.class);
            if (edgeInstallVersion != null) {
                edgeInstallInstructionsService.setAppVersion(edgeInstallVersion);
                edgeUpgradeInstructionsService.setAppVersion(edgeInstallVersion);
            }
            EdgeUpgradeMessage edgeUpgradeMessage = restClient.postForObject(UPDATE_SERVER_BASE_URL + "/api/v1/edge/upgradeMapping", new HttpEntity<>(edgeRequest.toString(), headers), EdgeUpgradeMessage.class);
            if (edgeUpgradeMessage != null) {
                edgeUpgradeInstructionsService.updateInstructionMap(edgeUpgradeMessage.getEdgeVersions());
            }
        } catch (Exception e) {
            log.trace(e.getMessage());
        }
    };

    /** 返回缓存的最新更新消息。 */
    @Override
    public UpdateMessage checkUpdates() {
        return updateMessage;
    }
}
