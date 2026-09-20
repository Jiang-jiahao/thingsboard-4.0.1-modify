package com.jnks.iot.server.transport.http.pull.session;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.HttpPullDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionInfoProto;
import com.jnks.iot.server.transport.http.pull.HttpPullTransportContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Builder
public class HttpPullCollectorSessionContext {

    private TenantId tenantId;
    private Device device;
    private DeviceProfile deviceProfile;
    private String token;
    private SessionInfoProto sessionInfo;
    private HttpPullDeviceProfileTransportConfiguration profileTransportConfiguration;
    private HttpPullDeviceTransportConfiguration deviceTransportConfiguration;
    private HttpPullTransportContext transportContext;

    @Builder.Default
    private final List<ScheduledTask> queryingTasks = new ArrayList<>();

    /** 轮询失败抑制状态（key = poll request id），随会话一起销毁。 */
    @Builder.Default
    private final Map<String, HttpPullPollFailureTracker> pollFailures = new ConcurrentHashMap<>();

    public DeviceId getDeviceId() {
        return device.getId();
    }

    public void close() {
        queryingTasks.forEach(ScheduledTask::cancel);
        queryingTasks.clear();
    }
}
