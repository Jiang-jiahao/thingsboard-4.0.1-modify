package org.thingsboard.server.transport.http.pull;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.transport.http.HttpPullAuthConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullAuthType;

import java.util.HashMap;
import java.util.Map;

/**
 * HTTP 出站 RPC 共享执行器：Pull 与 DEFAULT（被动）档案共用。
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class HttpOutboundRpcExecutor {

    private final HttpPullAuthService authService;
    private HttpPullHttpClient httpClient;

    void setHttpClient(HttpPullHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @PostConstruct
    public void init() {
        httpClient = new HttpPullHttpClient(10000);
    }

    public OutboundHttpResult execute(DeviceId authDeviceId,
                                      Device targetDevice,
                                      HttpPullDeviceTransportConfiguration targetDeviceCfg,
                                      HttpPullAuthConfiguration auth,
                                      DeviceProfileRpcMethod rpcMethod,
                                      String paramsJson,
                                      String urlOverride,
                                      int readTimeoutMs,
                                      int requestId) throws Exception {
        String url = HttpPullPollUrlResolver.resolve(rpcMethod.getHttpUrl(), urlOverride);
        url = HttpPullTemplateResolver.resolve(url, targetDevice, targetDeviceCfg, paramsJson,
                requestId, rpcMethod.getId());

        boolean requiresAuth = rpcMethod.getRequiresAuth() != null
                ? rpcMethod.getRequiresAuth()
                : auth != null && auth.getAuthType() != null && auth.getAuthType() != HttpPullAuthType.NONE;

        HttpPullAuthService.AuthRequestContext authCtx = authService.prepareAuth(
                authDeviceId, auth, url, requiresAuth, urlOverride, readTimeoutMs);

        Map<String, String> headers = buildResolvedHeaders(rpcMethod, targetDevice, targetDeviceCfg, paramsJson,
                requestId, authCtx);
        String body = HttpPullTemplateResolver.resolve(
                rpcMethod.getHttpBody(), targetDevice, targetDeviceCfg, paramsJson, requestId, rpcMethod.getId());
        if (!headers.containsKey("Content-Type") && StringUtils.isNotBlank(body)) {
            headers.put("Content-Type", "application/json");
        }

        log.info("[{}] HTTP outbound RPC [{}] {} {} body={}",
                authDeviceId, rpcMethod.getId(), rpcMethod.getHttpMethod(), url, truncate(body));

        HttpPullHttpClient.HttpPullResponse response = executeHttp(rpcMethod, authCtx, headers, body, readTimeoutMs);

        if (response.getStatusCode() == 401 && requiresAuth) {
            log.info("[{}] HTTP outbound RPC [{}] 401, refreshing login token",
                    authDeviceId, rpcMethod.getId());
            authService.invalidate(authDeviceId);
            authCtx = authService.prepareAuth(authDeviceId, auth, url, true, urlOverride, readTimeoutMs);
            headers = buildResolvedHeaders(rpcMethod, targetDevice, targetDeviceCfg, paramsJson, requestId, authCtx);
            if (!headers.containsKey("Content-Type") && StringUtils.isNotBlank(body)) {
                headers.put("Content-Type", "application/json");
            }
            response = executeHttp(rpcMethod, authCtx, headers, body, readTimeoutMs);
        }
        return new OutboundHttpResult(response.getStatusCode(), response.getBody());
    }

    private Map<String, String> buildResolvedHeaders(DeviceProfileRpcMethod rpcMethod,
                                                     Device targetDevice,
                                                     HttpPullDeviceTransportConfiguration targetDeviceCfg,
                                                     String paramsJson,
                                                     int requestId,
                                                     HttpPullAuthService.AuthRequestContext authCtx) {
        Map<String, String> headers = new HashMap<>();
        if (rpcMethod.getHttpHeaders() != null) {
            headers.putAll(HttpPullTemplateResolver.resolveHeaders(
                    rpcMethod.getHttpHeaders(), targetDevice, targetDeviceCfg, paramsJson, requestId, rpcMethod.getId()));
        }
        if (authCtx.getHeaders() != null) {
            headers.putAll(authCtx.getHeaders());
        }
        return headers;
    }

    private HttpPullHttpClient.HttpPullResponse executeHttp(DeviceProfileRpcMethod rpcMethod,
                                                            HttpPullAuthService.AuthRequestContext authCtx,
                                                            Map<String, String> headers,
                                                            String body,
                                                            int readTimeoutMs) throws Exception {
        return httpClient.execute(HttpPullHttpClient.HttpPullRequest.builder()
                .url(authCtx.getUrl())
                .method(rpcMethod.getHttpMethod())
                .body(body)
                .headers(headers)
                .queryParams(authCtx.getQueryParams())
                .readTimeoutMs(readTimeoutMs)
                .build());
    }

    public record OutboundHttpResult(int statusCode, String body) {
    }

    private static String truncate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() > 256 ? s.substring(0, 256) + "..." : s;
    }
}
