/**
 * Copyright © 2016-2025 The Thingsboard Authors
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 设备档案级「平台 RPC 方法」：对外统一 {@link #id}，按 {@link #bindingType} 映射到 TCP 模板下行或原生设备 RPC。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceProfileRpcMethod implements Serializable {

    /**
     * 平台侧 RPC 方法名（规则链、REST、控件调用 {@code method} 时使用）；字母开头，仅字母数字下划线。
     */
    private String id;
    private String displayName;
    private Boolean oneWay;
    /** 可选；毫秒 */
    private Long timeoutMs;

    private DeviceProfileRpcBindingType bindingType;

    // --- TCP_TEMPLATE（绑定协议模板包内下行/双向命令）---

    /**
     * 与 {@link ProtocolTemplateCommandDefinition#getName()} 一致，便于展示；下发定位以 commandValue + templateId 为准。
     */
    private String templateCommandName;
    private Long commandValue;
    private String templateId;
    /**
     * 平台参数名 → 模板字段 key（键不一致时映射；为空则同名传递）。
     */
    private Map<String, String> paramMap;

    // --- NATIVE（MQTT 等）---

    /**
     * 设备固件认识的 RPC {@code method}。
     */
    private String deviceMethod;
    /**
     * 可选：默认 params JSON 字符串（运维参考或预填；下发时可与调用方 params 合并）。
     */
    private String paramsTemplateJson;

    // --- HTTP_OUTBOUND（平台调用厂家 HTTP 接口）---

    private String httpUrl;
    private String httpMethod = "POST";
    private String httpBody;
    private Map<String, String> httpHeaders = new HashMap<>();
    private Boolean requiresAuth;

    // --- MQTT（NATIVE 可选主题 / MQTT_CUSTOM 必填主题与模板）---

    /**
     * 请求主题。支持 {@code ${device.name}}、{@code ${requestId}}、{@code ${params.xxx}} 等占位符。
     * MQTT 服务端 NATIVE 留空则使用标准 {@code v1/devices/me/rpc/request/{requestId}}。
     */
    private String mqttRequestTopic;
    /**
     * 双向 RPC 的响应主题。留空表示单向，或 MQTT 服务端 NATIVE 走标准 {@code v1/devices/me/rpc/response/{requestId}}。
     */
    private String mqttResponseTopic;
    /**
     * MQTT_CUSTOM 请求体模板。支持 {@code ${params}}、{@code ${params.xxx}}、{@code ${device.name}}、{@code ${requestId}}、{@code ${method}}。
     */
    private String mqttPayloadTemplate;
    /** 0/1/2，默认 1 */
    private Integer mqttQos;

    public void validate(DeviceProfileRpcBindingType expectedForTransport) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("device profile RPC method id is required");
        }
        if (!id.matches("[a-zA-Z][a-zA-Z0-9_]*")) {
            throw new IllegalArgumentException("device profile RPC method id must match [a-zA-Z][a-zA-Z0-9_]*: " + id);
        }
        if (bindingType == null) {
            throw new IllegalArgumentException("device profile RPC method bindingType is required for id=" + id);
        }
        if (expectedForTransport != null && bindingType != expectedForTransport) {
            throw new IllegalArgumentException("device profile RPC method " + id + " bindingType " + bindingType
                    + " does not match transport expectation " + expectedForTransport);
        }
        switch (bindingType) {
            case TCP_TEMPLATE, UDP_TEMPLATE -> {
                if (templateId == null || templateId.isBlank()) {
                    throw new IllegalArgumentException("Protocol template RPC method requires templateId: " + id);
                }
                if (commandValue == null) {
                    throw new IllegalArgumentException("Protocol template RPC method requires commandValue: " + id);
                }
            }
            case NATIVE -> {
                if (deviceMethod == null || deviceMethod.isBlank()) {
                    throw new IllegalArgumentException("NATIVE RPC method requires deviceMethod: " + id);
                }
            }
            case HTTP_OUTBOUND -> {
                if (httpUrl == null || httpUrl.isBlank()) {
                    throw new IllegalArgumentException("HTTP_OUTBOUND RPC method requires httpUrl: " + id);
                }
                if (httpMethod == null || httpMethod.isBlank()) {
                    throw new IllegalArgumentException("HTTP_OUTBOUND RPC method requires httpMethod: " + id);
                }
            }
            case MQTT_CUSTOM -> {
                if (mqttRequestTopic == null || mqttRequestTopic.isBlank()) {
                    throw new IllegalArgumentException("MQTT_CUSTOM RPC method requires mqttRequestTopic: " + id);
                }
                if (mqttPayloadTemplate == null || mqttPayloadTemplate.isBlank()) {
                    throw new IllegalArgumentException("MQTT_CUSTOM RPC method requires mqttPayloadTemplate: " + id);
                }
            }
        }
        if (mqttQos != null && (mqttQos < 0 || mqttQos > 2)) {
            throw new IllegalArgumentException("RPC method mqttQos must be 0, 1 or 2: " + id);
        }
    }
}
