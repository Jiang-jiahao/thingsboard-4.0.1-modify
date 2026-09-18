package org.thingsboard.server.service.rpc;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.event.TransactionalEventListener;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.device.data.DeviceScheduledRpc;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcBindingType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.data.rpc.RpcError;
import org.thingsboard.server.common.data.rpc.ToDeviceRpcRequestBody;
import org.thingsboard.server.common.msg.plugin.ComponentLifecycleMsg;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.rpc.ToDeviceRpcRequest;
import org.thingsboard.server.dao.audit.AuditLogService;
import org.thingsboard.server.dao.device.DeviceProfileService;
import org.thingsboard.server.dao.device.DeviceService;
import org.thingsboard.server.dao.eventsourcing.DeleteEntityEvent;
import org.thingsboard.server.dao.eventsourcing.SaveEntityEvent;
import org.thingsboard.server.dao.tenant.TenantService;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 按设备 {@code deviceData.scheduledRpcs} 在 Core 分区归属节点周期发起标准 to-device RPC。
 * 方法定义来自设备档案；开关/间隔在设备上。审计用户为 System。
 */
@Service
@Slf4j
public class DefaultDeviceProfileScheduledRpcService extends TbApplicationEventListener<PartitionChangeEvent> {

    private static final long DEFAULT_TIMEOUT_MS = 10_000L;
    private static final int PAGE_SIZE = 1000;
    private static final UserId SYSTEM_USER_ID = new UserId(DeviceScheduledRpc.SYSTEM_AUDIT_USER_UUID);

    private final DeviceService deviceService;
    private final DeviceProfileService deviceProfileService;
    private final TenantService tenantService;
    private final PartitionService partitionService;
    private final TbCoreDeviceRpcService deviceRpcService;
    private final AuditLogService auditLogService;

    private final Map<String, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler;

    public DefaultDeviceProfileScheduledRpcService(DeviceService deviceService,
                                                   DeviceProfileService deviceProfileService,
                                                   TenantService tenantService,
                                                   PartitionService partitionService,
                                                   @Lazy TbCoreDeviceRpcService deviceRpcService,
                                                   AuditLogService auditLogService) {
        this.deviceService = deviceService;
        this.deviceProfileService = deviceProfileService;
        this.tenantService = tenantService;
        this.partitionService = partitionService;
        this.deviceRpcService = deviceRpcService;
        this.auditLogService = auditLogService;
    }

    @PostConstruct
    public void init() {
        scheduler = ThingsBoardExecutors.newScheduledThreadPool(2, "tb-device-scheduled-rpc");
    }

    @PreDestroy
    public void destroy() {
        cancelAll();
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Override
    protected void onTbApplicationEvent(PartitionChangeEvent event) {
        if (event.getServiceType() != ServiceType.TB_CORE) {
            return;
        }
        ensureScheduler();
        log.info("Core partitions changed; rebuilding device scheduled RPC tasks");
        rebuildAll();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        ensureScheduler();
        log.info("Application ready; rebuilding device scheduled RPC tasks");
        rebuildAll();
    }

    private void ensureScheduler() {
        if (scheduler == null) {
            init();
        }
    }

    /**
     * 本地事务保存事件：仅当本节点持有设备分区时生效。
     * 其它 Core 节点通过 {@link #onComponentLifecycle} 收到集群广播后再挂任务。
     */
    @TransactionalEventListener(fallbackExecution = true)
    public void onEntitySaved(SaveEntityEvent<?> event) {
        if (Boolean.FALSE.equals(event.getBroadcastEvent()) || event.getEntity() == null) {
            return;
        }
        if (event.getEntity() instanceof DeviceProfile profile) {
            rescheduleProfile(profile);
        } else if (event.getEntity() instanceof Device device) {
            rescheduleDevice(device);
        }
    }

    @TransactionalEventListener(fallbackExecution = true)
    public void onEntityDeleted(DeleteEntityEvent<?> event) {
        if (event.getEntity() instanceof DeviceProfile profile) {
            cancelProfile(profile.getTenantId(), profile.getId());
        } else if (event.getEntity() instanceof Device device) {
            cancelDevice(device.getId());
        } else if (event.getEntityId() != null && event.getEntityId().getEntityType() != null) {
            switch (event.getEntityId().getEntityType()) {
                case DEVICE -> cancelDevice(new DeviceId(event.getEntityId().getId()));
                case DEVICE_PROFILE -> cancelProfile(event.getTenantId(), new DeviceProfileId(event.getEntityId().getId()));
                default -> {
                }
            }
        }
    }

    /**
     * 集群内各 Core 都会收到设备/档案生命周期广播。
     * 解决「在 8080 保存、但设备分区在另一台 Core」时本地 SaveEntityEvent 不挂定时任务的问题。
     */
    @EventListener(ComponentLifecycleMsg.class)
    public void onComponentLifecycle(ComponentLifecycleMsg event) {
        EntityId entityId = event.getEntityId();
        if (entityId == null || event.getEvent() == null) {
            return;
        }
        EntityType entityType = entityId.getEntityType();
        if (entityType == EntityType.DEVICE) {
            DeviceId deviceId = new DeviceId(entityId.getId());
            if (event.getEvent() == ComponentLifecycleEvent.DELETED) {
                cancelDevice(deviceId);
                return;
            }
            if (event.getEvent() == ComponentLifecycleEvent.CREATED
                    || event.getEvent() == ComponentLifecycleEvent.UPDATED) {
                Device device = deviceService.findDeviceById(event.getTenantId(), deviceId);
                if (device != null) {
                    rescheduleDevice(device);
                } else {
                    cancelDevice(deviceId);
                }
            }
        } else if (entityType == EntityType.DEVICE_PROFILE) {
            DeviceProfileId profileId = new DeviceProfileId(entityId.getId());
            if (event.getEvent() == ComponentLifecycleEvent.DELETED) {
                cancelProfile(event.getTenantId(), profileId);
                return;
            }
            if (event.getEvent() == ComponentLifecycleEvent.CREATED
                    || event.getEvent() == ComponentLifecycleEvent.UPDATED) {
                DeviceProfile profile = deviceProfileService.findDeviceProfileById(event.getTenantId(), profileId);
                if (profile != null) {
                    rescheduleProfile(profile);
                }
            }
        }
    }

    private void rebuildAll() {
        cancelAll();
        PageLink tenantLink = new PageLink(PAGE_SIZE);
        PageData<Tenant> tenants;
        do {
            tenants = tenantService.findTenants(tenantLink);
            for (Tenant tenant : tenants.getData()) {
                rebuildTenant(tenant.getId());
            }
            tenantLink = tenantLink.nextPageLink();
        } while (tenants.hasNext());
    }

    private void rebuildTenant(TenantId tenantId) {
        PageLink deviceLink = new PageLink(PAGE_SIZE);
        PageData<Device> devices;
        do {
            devices = deviceService.findDevicesByTenantId(tenantId, deviceLink);
            for (Device device : devices.getData()) {
                if (!isMyPartition(device.getTenantId(), device.getId())) {
                    continue;
                }
                scheduleDevice(device);
            }
            deviceLink = deviceLink.nextPageLink();
        } while (devices.hasNext());
    }

    private void rescheduleProfile(DeviceProfile profile) {
        cancelProfile(profile.getTenantId(), profile.getId());
        PageLink deviceLink = new PageLink(PAGE_SIZE);
        PageData<DeviceId> deviceIds;
        do {
            deviceIds = deviceService.findDeviceIdsByTenantIdAndDeviceProfileId(
                    profile.getTenantId(), profile.getId(), deviceLink);
            for (DeviceId deviceId : deviceIds.getData()) {
                Device device = deviceService.findDeviceById(profile.getTenantId(), deviceId);
                if (device != null) {
                    rescheduleDevice(device);
                }
            }
            deviceLink = deviceLink.nextPageLink();
        } while (deviceIds.hasNext());
    }

    private void rescheduleDevice(Device device) {
        cancelDevice(device.getId());
        if (!isMyPartition(device.getTenantId(), device.getId())) {
            return;
        }
        scheduleDevice(device);
    }

    private void scheduleDevice(Device device) {
        List<DeviceScheduledRpc> schedules = activeSchedules(device);
        if (schedules.isEmpty()) {
            return;
        }
        DeviceProfile profile = deviceProfileService.findDeviceProfileById(device.getTenantId(), device.getDeviceProfileId());
        if (profile == null) {
            return;
        }
        for (DeviceScheduledRpc schedule : schedules) {
            DeviceProfileRpcMethod method = findProfileMethod(profile, schedule.getMethodId());
            if (method == null) {
                log.debug("[{}] Scheduled RPC method [{}] not found on profile", device.getId(), schedule.getMethodId());
                continue;
            }
            scheduleTask(device, method, schedule);
        }
    }

    private void scheduleTask(Device device, DeviceProfileRpcMethod method, DeviceScheduledRpc schedule) {
        String key = taskKey(device.getId(), method.getId());
        cancelKey(key);
        long interval = schedule.getIntervalMs();
        TenantId tenantId = device.getTenantId();
        DeviceId deviceId = device.getId();
        CustomerId customerId = device.getCustomerId();
        String deviceName = device.getName();
        ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(
                () -> fireScheduledRpc(tenantId, deviceId, customerId, deviceName, method, schedule),
                interval, interval, TimeUnit.MILLISECONDS);
        tasks.put(key, future);
        log.info("[{}] Scheduled device RPC [{}] every {} ms", deviceId, method.getId(), interval);
    }

    private void fireScheduledRpc(TenantId tenantId, DeviceId deviceId, CustomerId customerId,
                                  String deviceName, DeviceProfileRpcMethod method, DeviceScheduledRpc schedule) {
        if (!isMyPartition(tenantId, deviceId)) {
            cancelKey(taskKey(deviceId, method.getId()));
            return;
        }
        String params = resolveScheduledParams(method, schedule);
        String rpcMethodName = resolveRpcMethodName(method);
        boolean oneWay = method.getOneWay() == null || Boolean.TRUE.equals(method.getOneWay());
        try {
            long timeoutMs = method.getTimeoutMs() != null && method.getTimeoutMs() > 0
                    ? method.getTimeoutMs() : DEFAULT_TIMEOUT_MS;
            ToDeviceRpcRequestBody body = new ToDeviceRpcRequestBody(rpcMethodName, params);
            ToDeviceRpcRequest request = new ToDeviceRpcRequest(
                    UUID.randomUUID(),
                    tenantId,
                    deviceId,
                    oneWay,
                    System.currentTimeMillis() + timeoutMs,
                    body,
                    false,
                    null,
                    "{\"source\":\"deviceSchedule\"}"
            );
            deviceRpcService.processRestApiRpcRequest(request, response -> {
                Optional<RpcError> error = response.getError();
                if (error.isPresent()) {
                    log.debug("[{}] Scheduled RPC [{}] finished with {}",
                            deviceId, method.getId(), error.get());
                } else {
                    log.trace("[{}] Scheduled RPC [{}] delivered", deviceId, method.getId());
                }
                logScheduledAudit(tenantId, customerId, deviceId, deviceName, oneWay, rpcMethodName, params, error);
            }, null);
        } catch (Exception e) {
            log.warn("[{}] Scheduled RPC [{}] failed to submit", deviceId, method.getId(), e);
            logScheduledAudit(tenantId, customerId, deviceId, deviceName, oneWay, rpcMethodName, params,
                    Optional.of(RpcError.INTERNAL));
        }
    }

    private void logScheduledAudit(TenantId tenantId, CustomerId customerId, DeviceId deviceId, String deviceName,
                                   boolean oneWay, String methodId, String params, Optional<RpcError> error) {
        try {
            String rpcErrorStr = error.map(e -> "RPC Error: " + e.name()).orElse("");
            Device named = new Device();
            named.setName(deviceName);
            named.setId(deviceId);
            named.setTenantId(tenantId);
            named.setCustomerId(customerId);
            auditLogService.logEntityAction(
                    tenantId,
                    customerId,
                    SYSTEM_USER_ID,
                    DeviceScheduledRpc.SYSTEM_AUDIT_USER_NAME,
                    deviceId,
                    named,
                    ActionType.RPC_CALL,
                    null,
                    rpcErrorStr,
                    oneWay,
                    methodId,
                    params
            );
        } catch (Exception e) {
            log.debug("[{}] Failed to write scheduled RPC audit log [{}]", deviceId, methodId, e);
        }
    }

    private static String resolveRpcMethodName(DeviceProfileRpcMethod method) {
        if (method.getBindingType() == DeviceProfileRpcBindingType.NATIVE
                && StringUtils.isNotBlank(method.getDeviceMethod())) {
            return method.getDeviceMethod();
        }
        return method.getId();
    }

    private static String resolveScheduledParams(DeviceProfileRpcMethod method, DeviceScheduledRpc schedule) {
        String template = StringUtils.isNotBlank(method.getParamsTemplateJson())
                ? method.getParamsTemplateJson().trim() : null;
        String override = schedule != null && StringUtils.isNotBlank(schedule.getParamsJson())
                ? schedule.getParamsJson().trim() : null;
        if (override == null) {
            return template != null ? template : "{}";
        }
        if (template == null) {
            return override;
        }
        // 定时 params 覆盖模板同名键，但保留模板里未改的键（如 topic 用的 deviceId）
        return mergeJsonObjects(template, override);
    }

    private static String mergeJsonObjects(String baseJson, String overrideJson) {
        try {
            JsonNode base = JacksonUtil.toJsonNode(baseJson);
            JsonNode over = JacksonUtil.toJsonNode(overrideJson);
            if (base == null || !base.isObject()) {
                return overrideJson;
            }
            if (over == null || !over.isObject()) {
                return baseJson;
            }
            ObjectNode merged = ((ObjectNode) base).deepCopy();
            over.fields().forEachRemaining(e -> merged.set(e.getKey(), e.getValue()));
            return JacksonUtil.toString(merged);
        } catch (Exception e) {
            return overrideJson;
        }
    }

    private static List<DeviceScheduledRpc> activeSchedules(Device device) {
        if (device.getDeviceData() == null || device.getDeviceData().getScheduledRpcs() == null) {
            return List.of();
        }
        List<DeviceScheduledRpc> out = new ArrayList<>();
        for (DeviceScheduledRpc scheduled : device.getDeviceData().getScheduledRpcs()) {
            if (scheduled != null && scheduled.isActive()) {
                out.add(scheduled);
            }
        }
        return out;
    }

    private static DeviceProfileRpcMethod findProfileMethod(DeviceProfile profile, String methodId) {
        if (StringUtils.isBlank(methodId) || profile.getProfileData() == null
                || profile.getProfileData().getRpcMethods() == null) {
            return null;
        }
        for (DeviceProfileRpcMethod method : profile.getProfileData().getRpcMethods()) {
            if (method != null && methodId.equals(method.getId())) {
                return method;
            }
        }
        return null;
    }

    private boolean isMyPartition(TenantId tenantId, DeviceId deviceId) {
        return partitionService.resolve(ServiceType.TB_CORE, tenantId, deviceId).isMyPartition();
    }

    private void cancelProfile(TenantId tenantId, DeviceProfileId profileId) {
        PageLink deviceLink = new PageLink(PAGE_SIZE);
        PageData<DeviceId> deviceIds;
        do {
            deviceIds = deviceService.findDeviceIdsByTenantIdAndDeviceProfileId(tenantId, profileId, deviceLink);
            for (DeviceId deviceId : deviceIds.getData()) {
                cancelDevice(deviceId);
            }
            deviceLink = deviceLink.nextPageLink();
        } while (deviceIds.hasNext());
    }

    private void cancelDevice(DeviceId deviceId) {
        String prefix = deviceId.getId() + ":";
        List<String> keys = tasks.keySet().stream().filter(k -> k.startsWith(prefix)).toList();
        keys.forEach(this::cancelKey);
    }

    private void cancelAll() {
        List<String> keys = new ArrayList<>(tasks.keySet());
        keys.forEach(this::cancelKey);
    }

    private void cancelKey(String key) {
        ScheduledFuture<?> future = tasks.remove(key);
        if (future != null) {
            future.cancel(false);
        }
    }

    private static String taskKey(DeviceId deviceId, String methodId) {
        return deviceId.getId() + ":" + methodId;
    }
}
