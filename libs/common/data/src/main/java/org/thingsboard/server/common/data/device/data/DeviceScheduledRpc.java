package org.thingsboard.server.common.data.device.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.thingsboard.server.common.data.StringUtils;

import java.io.Serializable;
import java.util.UUID;

/**
 * 设备级定时 RPC：引用档案 RPC 方法 {@code methodId}，由本设备单独开关与间隔。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceScheduledRpc implements Serializable {

    public static final long MIN_INTERVAL_MS = 1000L;
    public static final String SYSTEM_AUDIT_USER_NAME = "System";
    public static final UUID SYSTEM_AUDIT_USER_UUID = new UUID(0L, 0L);

    /** 档案 RPC 方法 id */
    private String methodId;
    private Boolean enabled;
    /** 间隔毫秒；启用时 {@code >= 1000} */
    private Long intervalMs;
    /**
     * 原生/MQTT 定时 params 原文（不是 {@code ${params.xxx}} 模板）。
     * HTTP_OUTBOUND 忽略，使用档案方法上的请求体原文。
     */
    private String paramsJson;

    public void validate() {
        if (StringUtils.isBlank(methodId)) {
            throw new IllegalArgumentException("Scheduled RPC methodId is required");
        }
        if (Boolean.TRUE.equals(enabled)) {
            if (intervalMs == null || intervalMs < MIN_INTERVAL_MS) {
                throw new IllegalArgumentException("Scheduled RPC intervalMs must be >= 1000: " + methodId);
            }
        }
    }

    @JsonIgnore
    public boolean isActive() {
        return Boolean.TRUE.equals(enabled)
                && StringUtils.isNotBlank(methodId)
                && intervalMs != null
                && intervalMs >= MIN_INTERVAL_MS;
    }
}
