package com.jnks.iot.server.transport.http.pull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileData;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcBindingType;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcMethod;
import com.jnks.iot.server.common.data.device.profile.HttpPullDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.transport.TransportDeviceProfileCache;
import com.jnks.iot.server.common.transport.TransportService;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionInfoProto;
import com.jnks.iot.server.transport.http.pull.session.HttpPullCollectorSessionContext;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HttpPullScheduledRpcTest {

    @Mock
    private TransportService transportService;
    @Mock
    private HttpPullAuthService authService;
    @Mock
    private TransportDeviceProfileCache deviceProfileCache;
    @Mock
    private HttpPullHttpClient httpClient;

    private HttpPullRpcService rpcService;
    private HttpPullCollectorSessionContext ctx;

    @BeforeEach
    void setUp() {
        HttpOutboundRpcExecutor executor = new HttpOutboundRpcExecutor(authService);
        executor.setHttpClient(httpClient);
        rpcService = new HttpPullRpcService(executor, transportService, deviceProfileCache);

        Device device = new Device();
        device.setId(new DeviceId(UUID.randomUUID()));
        device.setName("http-pull-device");
        DeviceProfileRpcMethod scheduled = new DeviceProfileRpcMethod();
        scheduled.setId("httpSetValue");
        scheduled.setBindingType(DeviceProfileRpcBindingType.HTTP_OUTBOUND);
        scheduled.setHttpUrl("http://127.0.0.1:19090/rpc");
        scheduled.setHttpMethod("POST");
        scheduled.setHttpBody("{\"cmd\":\"tick\",\"source\":\"schedule\"}");
        scheduled.setScheduleEnabled(true);
        scheduled.setScheduleIntervalMs(2000L);
        scheduled.setScheduleParamsJson("{\"cmd\":\"SHOULD_NOT_APPEAR\"}");

        DeviceProfileData profileData = new DeviceProfileData();
        profileData.setRpcMethods(List.of(scheduled));
        DeviceProfile profile = new DeviceProfile();
        profile.setProfileData(profileData);

        SessionInfoProto sessionInfo = SessionInfoProto.newBuilder()
                .setSessionIdMSB(UUID.randomUUID().getMostSignificantBits())
                .setSessionIdLSB(UUID.randomUUID().getLeastSignificantBits())
                .setDeviceIdMSB(device.getId().getId().getMostSignificantBits())
                .setDeviceIdLSB(device.getId().getId().getLeastSignificantBits())
                .build();
        ctx = HttpPullCollectorSessionContext.builder()
                .tenantId(TenantId.fromUUID(UUID.randomUUID()))
                .device(device)
                .deviceProfile(profile)
                .sessionInfo(sessionInfo)
                .profileTransportConfiguration(new HttpPullDeviceProfileTransportConfiguration())
                .deviceTransportConfiguration(new HttpPullDeviceTransportConfiguration())
                .build();
    }

    @Test
    void executeScheduledOutboundRpcUsesLiteralBody() throws Exception {
        when(authService.prepareAuth(any(), any(), anyString(), anyBoolean(), nullable(String.class), anyInt()))
                .thenReturn(HttpPullAuthService.AuthRequestContext.builder()
                        .url("http://127.0.0.1:19090/rpc")
                        .build());
        when(httpClient.execute(any())).thenReturn(HttpPullHttpClient.HttpPullResponse.builder()
                .statusCode(200)
                .body("{\"ok\":true}")
                .build());

        DeviceProfileRpcMethod method = ctx.getDeviceProfile().getProfileData().getRpcMethods().get(0);
        rpcService.executeScheduledOutboundRpc(ctx, method);

        ArgumentCaptor<HttpPullHttpClient.HttpPullRequest> captor =
                ArgumentCaptor.forClass(HttpPullHttpClient.HttpPullRequest.class);
        verify(httpClient).execute(captor.capture());
        assertThat(captor.getValue().getBody())
                .contains("tick")
                .doesNotContain("${params")
                .doesNotContain("SHOULD_NOT_APPEAR");
        verifyNoMoreInteractions(transportService);
    }
}
