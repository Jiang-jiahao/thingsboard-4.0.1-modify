package com.jnks.iot.server.service.protocoltemplate;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcBindingType;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcMethod;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateBundle;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateCommandDefinition;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateCommandDirection;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateTransportTcpDataConfiguration;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateTransportUdpDataConfiguration;
import com.jnks.iot.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.TcpHexCommandProfile;
import com.jnks.iot.server.common.data.device.profile.TcpHexValueType;
import com.jnks.iot.server.common.data.device.profile.UdpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 平台自己发起的 RPC（定时任务等，不经管理端）在服务端按协议模板组 HEX 帧。
 * <p>
 * 管理端手动下发时由前端调 {@code buildHex} 组帧，再把结果作为 {@code params.hex} 投递；
 * 定时 RPC 没有前端，必须在这一层补上，否则传输层拿不到 hex 会退回 RPC 信封 JSON。
 */
@Service
@RequiredArgsConstructor
public class ProtocolTemplateRpcFramer {

    private static final String HEX_PARAM_KEY = "hex";

    private final DefaultProtocolTemplateBundleService bundleService;
    private final ProtocolTemplateHexBuildService hexBuildService;

    /**
     * @return 组好帧的 {@code {"hex": "..."}}；非模板绑定或调用方已带 hex 时原样返回
     * @throws IllegalArgumentException 组帧失败（缺模板包、参数非法等），调用方应放弃本次下发
     */
    public String frameIfNeeded(TenantId tenantId, DeviceProfile profile, DeviceProfileRpcMethod method, String params) {
        if (method == null
                || (method.getBindingType() != DeviceProfileRpcBindingType.TCP_TEMPLATE
                && method.getBindingType() != DeviceProfileRpcBindingType.UDP_TEMPLATE)) {
            return params;
        }
        // 传输层按「params 是否含 hex 键」判别是否已是组好的帧，这里保持同一判据，避免二次组帧
        if (params != null && params.contains("\"" + HEX_PARAM_KEY + "\"")) {
            return params;
        }
        String bundleId = protocolTemplateBundleIdOf(profile);
        if (bundleId == null) {
            throw new IllegalArgumentException("RPC method [" + method.getId()
                    + "] is bound to a protocol template but the device profile has no protocolTemplateBundleId");
        }
        ProtocolTemplateBundle bundle;
        try {
            bundle = bundleService.findById(tenantId, UUID.fromString(bundleId))
                    .orElseThrow(() -> new IllegalArgumentException("Protocol template bundle not found: " + bundleId));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid protocolTemplateBundleId: " + bundleId, e);
        }

        ProtocolTemplateHexBuildRequest request = new ProtocolTemplateHexBuildRequest();
        request.setBundleId(bundleId);
        request.setTemplateId(method.getTemplateId());
        request.setCommandName(method.getTemplateCommandName());
        ProtocolTemplateCommandDefinition cmd = findCommand(bundle, method);
        if (cmd != null && TcpHexCommandProfile.isByteSliceCommandMatchType(
                cmd.getMatchValueType() != null ? cmd.getMatchValueType() : TcpHexValueType.UINT32_LE)) {
            request.setCommandMatchBytesHex(cmd.getCommandMatchBytesHex());
        } else {
            request.setCommandValue(method.getCommandValue());
        }
        request.setValues(toTemplateValues(method, params));

        ProtocolTemplateHexBuildResult result = hexBuildService.build(tenantId, request);
        if (!result.isSuccess() || StringUtils.isBlank(result.getHex())) {
            throw new IllegalArgumentException("Failed to build protocol template frame for RPC method ["
                    + method.getId() + "]: " + result.getErrorMessage());
        }
        return "{\"" + HEX_PARAM_KEY + "\":\"" + result.getHex() + "\"}";
    }

    private static String protocolTemplateBundleIdOf(DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null) {
            return null;
        }
        DeviceProfileTransportConfiguration cfg = profile.getProfileData().getTransportConfiguration();
        String id = null;
        if (cfg instanceof TcpDeviceProfileTransportConfiguration tcp
                && tcp.getTransportTcpDataTypeConfiguration() instanceof ProtocolTemplateTransportTcpDataConfiguration pt) {
            id = pt.getProtocolTemplateBundleId();
        } else if (cfg instanceof UdpDeviceProfileTransportConfiguration udp
                && udp.getTransportUdpDataTypeConfiguration() instanceof ProtocolTemplateTransportUdpDataConfiguration pt) {
            id = pt.getProtocolTemplateBundleId();
        }
        return StringUtils.isBlank(id) ? null : id.trim();
    }

    /** 与前端 {@code findBundleCommand} 一致：templateId + commandValue（+ 命令名消歧）。 */
    private static ProtocolTemplateCommandDefinition findCommand(ProtocolTemplateBundle bundle, DeviceProfileRpcMethod method) {
        List<ProtocolTemplateCommandDefinition> cmds = bundle.getProtocolCommands();
        if (cmds == null || method.getTemplateId() == null || method.getCommandValue() == null) {
            return null;
        }
        String name = StringUtils.isBlank(method.getTemplateCommandName())
                ? null : method.getTemplateCommandName().trim();
        for (ProtocolTemplateCommandDefinition c : cmds) {
            if (c == null) {
                continue;
            }
            if (c.getDirection() != ProtocolTemplateCommandDirection.DOWNLINK
                    && c.getDirection() != ProtocolTemplateCommandDirection.BOTH) {
                continue;
            }
            if (!method.getTemplateId().equals(c.getTemplateId())
                    || c.getCommandValue() != method.getCommandValue().longValue()) {
                continue;
            }
            if (name != null && !name.equals(c.getName())) {
                continue;
            }
            return c;
        }
        return null;
    }

    /**
     * 调用方 params 用平台参数名，组帧要的是模板字段 key —— 按 {@code paramMap} 转换，未映射的同名传递。
     */
    private static Map<String, Object> toTemplateValues(DeviceProfileRpcMethod method, String params) {
        Map<String, Object> values = new HashMap<>();
        if (StringUtils.isBlank(params)) {
            return values;
        }
        JsonNode node = JacksonUtil.toJsonNode(params);
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("RPC params for protocol template binding must be a JSON object: " + params);
        }
        Map<String, String> paramMap = method.getParamMap();
        node.fields().forEachRemaining(e -> {
            String templateKey = paramMap != null ? paramMap.getOrDefault(e.getKey(), e.getKey()) : e.getKey();
            values.put(templateKey, plainValue(e.getValue()));
        });
        return values;
    }

    private static Object plainValue(JsonNode v) {
        if (v == null || v.isNull()) {
            return null;
        }
        if (v.isNumber()) {
            return v.numberValue();
        }
        if (v.isBoolean()) {
            return v.booleanValue();
        }
        if (v.isTextual()) {
            return v.asText();
        }
        // 对象/数组组帧侧不支持，原样传入让它按字段名报可读错误
        return v.toString();
    }
}
