/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.StringUtils;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class HttpPullHttpClient {

    private final HttpClient httpClient;

    public HttpPullHttpClient(int connectTimeoutMs) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(Math.max(connectTimeoutMs, 1000)))
                .build();
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
