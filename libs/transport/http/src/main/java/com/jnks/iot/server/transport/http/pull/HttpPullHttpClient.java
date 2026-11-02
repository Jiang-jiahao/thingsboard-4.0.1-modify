package com.jnks.iot.server.transport.http.pull;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.StringUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class HttpPullHttpClient {

    private final HttpClient httpClient;

    public HttpPullHttpClient(int connectTimeoutMs) {
        this(connectTimeoutMs, false);
    }

    /**
     * @param insecureSsl true 时跳过证书链校验，用于内网自签证书的设备
     *                    （由 {@code transport.http.pull.ssl.insecure} 控制，默认 true）
     */
    public HttpPullHttpClient(int connectTimeoutMs, boolean insecureSsl) {
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(Math.max(connectTimeoutMs, 1000)));
        if (insecureSsl) {
            // 自签证书的 CN/SAN 常与设备 IP 不一致，关闭主机名校验需在 JVM 启动时传
            // -Djdk.internal.httpclient.disableHostnameVerification=true；这里尽力设置，已有连接不受影响。
            System.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true");
            builder.sslContext(insecureSslContext());
            log.warn("HTTP pull TLS certificate verification is disabled (transport.http.pull.ssl.insecure=true)");
        }
        this.httpClient = builder.build();
    }

    private static SSLContext insecureSslContext() {
        try {
            TrustManager[] trustAll = new TrustManager[]{new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            }};
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trustAll, new SecureRandom());
            return context;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build insecure SSL context for HTTP pull", e);
        }
    }

    public HttpPullResponse execute(HttpPullRequest request) throws Exception {
        String method = request.getMethod() != null ? request.getMethod().toUpperCase() : "GET";
        URI uri = buildUri(request.getUrl(), request.getQueryParams());
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofMillis(Math.max(request.getReadTimeoutMs(), 1000)));
        if (request.getHeaders() != null) {
            request.getHeaders().forEach(builder::header);
        }
        String body = request.getBody();
        switch (method) {
            case "POST" -> builder.POST(bodyPublisher(body));
            case "PUT" -> builder.PUT(bodyPublisher(body));
            case "PATCH" -> builder.method("PATCH", bodyPublisher(body));
            case "DELETE" -> builder.method("DELETE", bodyPublisher(body));
            default -> builder.GET();
        }
        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return HttpPullResponse.builder()
                .statusCode(response.statusCode())
                .body(response.body())
                .build();
    }

    /**
     * HTTP 头名不区分大小写，但 {@link HttpRequest.Builder#header} 会为大小写不同的同名头各加一个值，
     * 导致发出两个 Content-Type。配置里的键是人工填的，比较时必须忽略大小写。
     */
    public static boolean hasHeader(Map<String, String> headers, String name) {
        if (headers == null || headers.isEmpty()) {
            return false;
        }
        return headers.keySet().stream().anyMatch(k -> k != null && k.equalsIgnoreCase(name));
    }

    private static HttpRequest.BodyPublisher bodyPublisher(String body) {
        if (StringUtils.isBlank(body)) {
            return HttpRequest.BodyPublishers.noBody();
        }
        return HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8);
    }

    private static URI buildUri(String url, Map<String, String> queryParams) throws Exception {
        if (queryParams == null || queryParams.isEmpty()) {
            return URI.create(url);
        }
        String qs = queryParams.entrySet().stream()
                .map(e -> URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
        String sep = url.contains("?") ? "&" : "?";
        return URI.create(url + sep + qs);
    }

    @Data
    @Builder
    public static class HttpPullRequest {
        private String url;
        private String method;
        private String body;
        private Map<String, String> headers;
        private Map<String, String> queryParams;
        private int readTimeoutMs;
    }

    @Data
    @Builder
    public static class HttpPullResponse {
        private int statusCode;
        private String body;
    }
}
