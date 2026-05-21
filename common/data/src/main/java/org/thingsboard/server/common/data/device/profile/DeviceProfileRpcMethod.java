/**
 * Copyright © 2016-2025 The Thingsboard Authors
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serializable;
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
        }
    }
}
