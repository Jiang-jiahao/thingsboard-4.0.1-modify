package com.jnks.iot.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.transport.http.HttpPullAuthConfiguration;
import com.jnks.iot.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import com.jnks.iot.server.common.data.transport.http.HttpPullPollRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class HttpPullDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    /**
     * UI 工作模式标记：PULL=主动拉取（与 {@link #getType()} 一致，便于 JSON 多态缺失时回显）。
     * <p>
     * 刻意**不给字段默认值**：工作模式必须由调用方显式声明（与"HTTP 工作模式必须手选"的口径一致），
     * 缺省时应被 {@link #isValid()} 判为无效，而不是静默落到 PULL。
     */
    private String httpTransportMode;

    private Integer timeoutMs = 10000;
    private Integer readTimeoutMs = 10000;
    private Long queryingFrequencyMs = 30000L;

    private List<HttpPullPollRequest> pollRequests = new ArrayList<>();

    @Deprecated
    private String pollUrl;
    @Deprecated
    private String pollMethod = "GET";
    @Deprecated
    private String pollBody;
    @Deprecated
    private Map<String, String> pollHeaders = new HashMap<>();

    /** 档案级登录/令牌配置，供 requiresAuth=true 的拉取请求共用 */
    private HttpPullAuthConfiguration auth = new HttpPullAuthConfiguration();

    /** 兼容旧配置；HTTP 拉取不再做多设备路由 */
    @Deprecated
    private HttpPullDeviceRoutingConfiguration routing;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.HTTP_PULL;
    }

    @Override
    public void validate() {
        if (!isValid()) {
            throw new IllegalArgumentException("HTTP pull transport configuration is not valid");
        }
    }

    @JsonIgnore
    public List<HttpPullPollRequest> effectivePollRequests() {
        if (pollRequests != null && !pollRequests.isEmpty()) {
            return pollRequests.stream().filter(HttpPullPollRequest::isRequestEnabled).toList();
        }
        if (StringUtils.isNotBlank(pollUrl)) {
            return List.of(HttpPullPollRequest.fromLegacyProfile(this));
        }
        return List.of();
    }

    @JsonIgnore
    public long resolveQueryingFrequencyMs(HttpPullPollRequest request) {
        if (request.getQueryingFrequencyMs() != null && request.getQueryingFrequencyMs() > 0) {
            return request.getQueryingFrequencyMs();
        }
        return queryingFrequencyMs != null && queryingFrequencyMs > 0 ? queryingFrequencyMs : 30000L;
    }

    @JsonIgnore
    private boolean isValid() {
        if (StringUtils.isBlank(httpTransportMode)) {
            return false;
        }
        if (timeoutMs == null || timeoutMs < 0 || readTimeoutMs == null || readTimeoutMs < 0) {
            return false;
        }
        // 与请求级（HttpPullPollRequest）和前端一致：档案级轮询间隔下限也是 1000ms
        if (queryingFrequencyMs == null || queryingFrequencyMs < 1000) {
            return false;
        }
        List<HttpPullPollRequest> requests = effectivePollRequests();
        if (requests.isEmpty()) {
            return false;
        }
        for (HttpPullPollRequest request : requests) {
            request.validate();
        }
        if (auth != null) {
            auth.validate();
        }
        return true;
    }
}
