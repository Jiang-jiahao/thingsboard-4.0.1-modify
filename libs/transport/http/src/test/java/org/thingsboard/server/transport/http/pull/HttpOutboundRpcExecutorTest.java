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
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcBindingType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.data.id.DeviceId;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HttpOutboundRpcExecutorTest {

    @Mock
    private HttpPullAuthService authService;
    @Mock
    private HttpPullHttpClient httpClient;

    private HttpOutboundRpcExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new HttpOutboundRpcExecutor(authService);
        executor.setHttpClient(httpClient);
    }

    @Test
    void executeWithoutProfileAuthPostsResolvedBody() throws Exception {
        when(authService.prepareAuth(any(), any(), anyString(), anyBoolean(), nullable(String.class), anyInt()))
                .thenReturn(HttpPullAuthService.AuthRequestContext.builder()
                        .url("http://127.0.0.1:19090/rpc")
                        .build());
        when(httpClient.execute(any())).thenReturn(new HttpPullHttpClient.HttpPullResponse(200, "{\"ok\":true}"));

        Device device = new Device();
        device.setId(new DeviceId(UUID.randomUUID()));
        device.setName("passive-1");

        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("httpSet");
        method.setBindingType(DeviceProfileRpcBindingType.HTTP_OUTBOUND);
        method.setHttpUrl("http://127.0.0.1:19090/rpc");
        method.setHttpMethod("POST");
        method.setHttpBody("{\"device\":\"${device.name}\",\"cmd\":\"${params.cmd}\",\"rid\":\"${requestId}\",\"m\":\"${method}\"}");

        HttpOutboundRpcExecutor.OutboundHttpResult result = executor.execute(
                device.getId(), device, null, null, method, "{\"cmd\":\"go\"}", null, 5000, 42);

        assertThat(result.statusCode()).isEqualTo(200);
        ArgumentCaptor<HttpPullHttpClient.HttpPullRequest> captor = ArgumentCaptor.forClass(HttpPullHttpClient.HttpPullRequest.class);
        verify(httpClient).execute(captor.capture());
        assertThat(captor.getValue().getBody()).contains("\"device\":\"passive-1\"");
        assertThat(captor.getValue().getBody()).contains("\"cmd\":\"go\"");
        assertThat(captor.getValue().getBody()).contains("\"rid\":\"42\"");
        assertThat(captor.getValue().getBody()).contains("\"m\":\"httpSet\"");
        assertThat(captor.getValue().getMethod()).isEqualTo("POST");
    }
}
