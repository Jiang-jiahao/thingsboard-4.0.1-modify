/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.HttpPullDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;
import org.thingsboard.server.common.data.transport.http.HttpPullPollRequest;
import org.thingsboard.server.common.data.transport.http.HttpPullRoutingMode;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.gen.transport.TransportProtos.PostAttributeMsg;
import org.thingsboard.server.gen.transport.TransportProtos.PostTelemetryMsg;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.transport.http.pull.session.HttpPullCollectorSessionContext;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HttpPullTransportServiceTest {

    private static final String MULTI_DEVICE_BODY = """
            [{"deviceId":"ext-a","temp":1},{"deviceId":"ext-b","temp":2}]
            """;

    @Mock
    private TransportService transportService;
    @Mock
    private HttpPullAuthService authService;
    @Mock
    private HttpPullHttpClient httpClient;

    private HttpPullTransportService service;
    private HttpPullCollectorSessionContext ctx;
    private SessionInfoProto sessionInfo;

    @BeforeEach
    void setUp() {
        service = new HttpPullTransportService(transportService, authService);
        service.setHttpClient(httpClient);
        Device device = new Device();
        device.setId(new DeviceId(UUID.randomUUID()));
        device.setName("http-pull-device");
        sessionInfo = SessionInfoProto.newBuilder()
                .setSessionIdMSB(UUID.randomUUID().getMostSignificantBits())
                .setSessionIdLSB(UUID.randomUUID().getLeastSignificantBits())
                .setDeviceIdMSB(device.getId().getId().getMostSignificantBits())
                .setDeviceIdLSB(device.getId().getId().getLeastSignificantBits())
                .build();
        ctx = HttpPullCollectorSessionContext.builder()
                .tenantId(TenantId.fromUUID(UUID.randomUUID()))
                .device(device)
                .sessionInfo(sessionInfo)
                .profileTransportConfiguration(new HttpPullDeviceProfileTransportConfiguration())
                .deviceTransportConfiguration(new HttpPullDeviceTransportConfiguration())
                .build();
    }

    @Test
    void telemetryArrayIsWrittenOnlyToPollingDevice() {
        HttpPullPollRequest request = telemetryRequest("detect");
        service.dispatchResponse(ctx, request, MULTI_DEVICE_BODY);

        ArgumentCaptor<PostTelemetryMsg> captor = ArgumentCaptor.forClass(PostTelemetryMsg.class);
        verify(transportService, times(1)).process(eq(sessionInfo), captor.capture(), isNull());
        PostTelemetryMsg msg = captor.getValue();
        assertThat(msg.getTsKvListCount()).isEqualTo(1);
        var kv = msg.getTsKvList(0).getKvList().stream()
                .filter(e -> "detect".equals(e.getKey()))
                .findFirst()
                .orElseThrow();
        assertThat(kv.getJsonV()).contains("ext-a");
        assertThat(kv.getJsonV()).contains("ext-b");
    }

    @Test
    void oldMultiDeviceRoutingConfigDoesNotSplitPayload() {
        HttpPullPollRequest request = telemetryRequest(null);
        HttpPullDeviceRoutingConfiguration routing = new HttpPullDeviceRoutingConfiguration();
        routing.setRoutingMode(HttpPullRoutingMode.MULTI_DEVICE);
        routing.setDeviceIdJsonPath("deviceId");
        routing.setTelemetryPayloadKey("legacyKey");
        request.setRouting(routing);

        service.dispatchResponse(ctx, request, MULTI_DEVICE_BODY);

        ArgumentCaptor<PostTelemetryMsg> captor = ArgumentCaptor.forClass(PostTelemetryMsg.class);
        verify(transportService, times(1)).process(eq(sessionInfo), captor.capture(), isNull());
        assertThat(captor.getValue().getTsKvList(0).getKvList()).anyMatch(e -> "legacyKey".equals(e.getKey()));
    }

    @Test
    void attributesWrittenOnceToPollingDevice() {
        HttpPullPollRequest request = new HttpPullPollRequest();
        request.setDataType(HttpPullPollDataType.SHARED_ATTRIBUTES);
        service.dispatchResponse(ctx, request, "{\"rssi\":-70}");

        ArgumentCaptor<PostAttributeMsg> captor = ArgumentCaptor.forClass(PostAttributeMsg.class);
        verify(transportService, times(1)).process(eq(sessionInfo), captor.capture(), isNull());
        assertThat(captor.getValue().getShared()).isTrue();
        assertThat(captor.getValue().getKvList()).anyMatch(e -> "rssi".equals(e.getKey()));
    }

    @Test
    void successfulPollPostsTelemetryToSelf() throws Exception {
        HttpPullPollRequest request = telemetryRequest("httpPullPayload");
        request.setPollUrl("http://192.168.1.10/api/data");
        request.setPollMethod("GET");
        ctx.getProfileTransportConfiguration().setPollRequests(List.of(request));
        when(authService.prepareAuth(any(), any(), anyString(), anyBoolean(), nullable(String.class)))
                .thenReturn(HttpPullAuthService.AuthRequestContext.builder()
                        .url("http://192.168.1.10/api/data")
                        .build());
        when(httpClient.execute(any())).thenReturn(HttpPullHttpClient.HttpPullResponse.builder()
                .statusCode(200)
                .body("{\"ok\":true}")
                .build());

        service.executePoll(ctx, request).get();

        verify(transportService).process(eq(sessionInfo), any(PostTelemetryMsg.class), isNull());
        verify(transportService, never()).errorEvent(any(), any(), any(), any());
    }

    @Test
    void failedPollReportsErrorWithoutTelemetry() throws Exception {
        HttpPullPollRequest request = telemetryRequest("httpPullPayload");
        request.setPollUrl("http://192.168.1.10/api/data");
        request.setPollMethod("GET");
        ctx.getProfileTransportConfiguration().setPollRequests(List.of(request));
        when(authService.prepareAuth(any(), any(), anyString(), anyBoolean(), nullable(String.class)))
                .thenReturn(HttpPullAuthService.AuthRequestContext.builder()
                        .url("http://192.168.1.10/api/data")
                        .build());
        when(httpClient.execute(any())).thenReturn(HttpPullHttpClient.HttpPullResponse.builder()
                .statusCode(500)
                .body("boom")
                .build());

        service.executePoll(ctx, request).get();

        verify(transportService).errorEvent(eq(ctx.getTenantId()), eq(ctx.getDeviceId()), eq("httpPullPoll"), any());
        verify(transportService, never()).process(any(SessionInfoProto.class), any(PostTelemetryMsg.class), isNull());
    }

    private static HttpPullPollRequest telemetryRequest(String key) {
        HttpPullPollRequest request = new HttpPullPollRequest();
        request.setName("poll-1");
        request.setDataType(HttpPullPollDataType.TELEMETRY);
        request.setTelemetryPayloadKey(key);
        return request;
    }
}
