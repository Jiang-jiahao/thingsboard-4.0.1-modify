/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.thingsboard.server.common.data.TransportTcpDataType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 协议模板负载：先定义 {@link #protocolTemplates 帧模板}，再按模板配置
 * {@link #protocolCommands 上行/下行命令}。上行命令展开为 {@link HexTransportTcpDataConfiguration} 供 TCP HEX <b>解析</b>；
 * 下行命令的字段定义仅作<strong>组帧参数契约</strong>持久化，不参与当前 TCP 侧解析展开。
 */
@Data
public class ProtocolTemplateTransportTcpDataConfiguration implements TransportTcpDataTypeConfiguration {

    /**
     * 可选：前端所选「协议模板包」ID，仅用于 UI 回显；传输展开不依赖此字段。
     */
    @JsonProperty("protocolTemplateBundleId")
    @JsonAlias({"monitoringProtocolBundleId"})
    private String protocolTemplateBundleId;

    @JsonProperty("protocolTemplates")
    @JsonAlias({"monitoringTemplates"})
    private List<ProtocolTemplateDefinition> protocolTemplates;

    @JsonProperty("protocolCommands")
    @JsonAlias({"monitoringCommands"})
    private List<ProtocolTemplateCommandDefinition> protocolCommands;

    @Override
    public TransportTcpDataType getTransportTcpDataType() {
        return TransportTcpDataType.PROTOCOL_TEMPLATE;
    }

    /**
     * 展开为与 {@link TcpHexProtocolParser} 兼容的 HEX 配置（仅含上行/双向命令规则）。
     */
    public HexTransportTcpDataConfiguration expandToHexTransportTcpDataConfiguration() {
        HexTransportTcpDataConfiguration hex = new HexTransportTcpDataConfiguration();
        List<ProtocolTemplateDefinition> templates = protocolTemplates != null ? protocolTemplates : List.of();
        if (!templates.isEmpty()) {
            ProtocolTemplateDefinition first = templates.get(0);
            hex.setHexProtocolFields(copyFieldList(first.getHexProtocolFields()));
            hex.setHexLtvRepeating(first.getHexLtvRepeating());
            hex.setChecksum(first.getChecksum());
        }
        List<TcpHexCommandProfile> profiles = new ArrayList<>();
        if (protocolCommands != null) {
            for (ProtocolTemplateCommandDefinition cmd : protocolCommands) {
                if (cmd == null) {
                    continue;
                }
                if (cmd.getDirection() == ProtocolTemplateCommandDirection.DOWNLINK) {
                    continue;
                }
                ProtocolTemplateDefinition tpl = findTemplate(cmd.getTemplateId());
                TcpHexCommandProfile p = toCommandProfile(cmd, tpl);
                profiles.add(p);
            }
        }
        hex.setHexCommandProfiles(profiles.isEmpty() ? null : profiles);
        return hex;
    }

    public ProtocolTemplateDefinition findTemplate(String templateId) {
        if (templateId == null || protocolTemplates == null) {
            return null;
        }
        for (ProtocolTemplateDefinition t : protocolTemplates) {
            if (t != null && templateId.equals(t.getId())) {
                return t;
            }
        }
        return null;
    }

    private TcpHexCommandProfile toCommandProfile(ProtocolTemplateCommandDefinition cmd,
                                                  ProtocolTemplateDefinition tpl) {
        if (tpl == null) {
            throw new IllegalArgumentException("Unknown protocol template templateId: " + cmd.getTemplateId());
        }
        TcpHexCommandProfile p = new TcpHexCommandProfile();
        if (cmd.getName() != null && !cmd.getName().isBlank()) {
            p.setName(cmd.getName());
        }
        int off = tpl.getCommandByteOffset() != null ? tpl.getCommandByteOffset() : 12;
        p.setMatchByteOffset(off);
        TcpHexValueType vt = cmd.getMatchValueType() != null ? cmd.getMatchValueType() : TcpHexValueType.UINT32_LE;
        p.setMatchValueType(vt);
        p.setMatchValue(cmd.getCommandValue());
        if (cmd.getSecondaryMatchByteOffset() != null) {
            p.setSecondaryMatchByteOffset(cmd.getSecondaryMatchByteOffset());
            p.setSecondaryMatchValueType(cmd.getSecondaryMatchValueType() != null ? cmd.getSecondaryMatchValueType()
                    : TcpHexValueType.UINT8);
            p.setSecondaryMatchValue(cmd.getSecondaryMatchValue());
        }

        p.setFields(mergeTemplateAndCommandFields(tpl.getHexProtocolFields(), cmd.getFields()));

        TcpHexLtvRepeatingConfig ltv = cmd.getLtvRepeating() != null ? cmd.getLtvRepeating() : tpl.getHexLtvRepeating();
        p.setLtvRepeating(ltv);
        return p;
    }

    private static List<TcpHexFieldDefinition> copyFieldList(List<TcpHexFieldDefinition> src) {
        if (src == null || src.isEmpty()) {
            return null;
        }
        return new ArrayList<>(src);
    }

    /**
     * 模板字段 + 命令增量字段：命令字段与模板字段按「占用字节区间」去重，重叠时以命令字段为准。
     */
    public static List<TcpHexFieldDefinition> mergeTemplateAndCommandFields(List<TcpHexFieldDefinition> templateFields,
                                                                            List<TcpHexFieldDefinition> commandFields) {
        if (commandFields == null || commandFields.isEmpty()) {
            return copyFieldList(templateFields);
        }
        List<TcpHexFieldDefinition> cmd = new ArrayList<>();
        for (TcpHexFieldDefinition c : commandFields) {
            if (c != null) {
                cmd.add(c);
            }
        }
        if (cmd.isEmpty()) {
            return copyFieldList(templateFields);
        }
        if (templateFields == null || templateFields.isEmpty()) {
            return cmd;
        }
        List<TcpHexFieldDefinition> merged = new ArrayList<>();
        for (TcpHexFieldDefinition t : templateFields) {
            if (t == null) {
                continue;
            }
            boolean overlapsAnyCommand = false;
            for (TcpHexFieldDefinition c : cmd) {
                if (hexFieldSpansOverlap(t, c)) {
                    overlapsAnyCommand = true;
                    break;
                }
            }
            if (!overlapsAnyCommand) {
                merged.add(t);
            }
        }
        merged.addAll(cmd);
        return merged;
    }

    /**
     * 两字段在帧内占用的字节区间是否相交（半开区间 [off, off+len)）。
     */
    static boolean hexFieldSpansOverlap(TcpHexFieldDefinition a, TcpHexFieldDefinition b) {
        int a0 = a.getByteOffset();
        int b0 = b.getByteOffset();
        int alen = fixedFieldByteLength(a);
        int blen = fixedFieldByteLength(b);
        if (alen <= 0 || blen <= 0) {
            return a0 == b0;
        }
        int a1 = a0 + alen;
        int b1 = b0 + blen;
        return a0 < b1 && b0 < a1;
    }

    private static int fixedFieldByteLength(TcpHexFieldDefinition f) {
        if (f == null || f.getValueType() == null) {
            return 0;
        }
        if (f.getValueType().isBytesAsHex()) {
            if (f.getByteLength() != null && f.getByteLength() > 0) {
                return f.getByteLength();
            }
            return -1;
        }
        return f.getValueType().getFixedByteLength();
    }

    public void validateProtocolTemplate() {
        if (protocolTemplates == null || protocolTemplates.isEmpty()) {
            throw new IllegalArgumentException("protocolTemplates must not be empty");
        }
        Set<String> ids = new HashSet<>();
        for (ProtocolTemplateDefinition t : protocolTemplates) {
            if (t == null) {
                continue;
            }
            t.validate();
            if (ids.contains(t.getId())) {
                throw new IllegalArgumentException("duplicate protocol template id: " + t.getId());
            }
            ids.add(t.getId());
        }
        if (protocolCommands == null || protocolCommands.isEmpty()) {
            throw new IllegalArgumentException("protocolCommands must not be empty");
        }
        for (ProtocolTemplateCommandDefinition c : protocolCommands) {
            if (c == null) {
                continue;
            }
            c.validate();
            if (findTemplate(c.getTemplateId()) == null) {
                throw new IllegalArgumentException("protocol template command references unknown templateId: " + c.getTemplateId());
            }
            if (c.getDirection() == ProtocolTemplateCommandDirection.DOWNLINK) {
                continue;
            }
            ProtocolTemplateDefinition tpl = findTemplate(c.getTemplateId());
            List<TcpHexFieldDefinition> mf = mergeTemplateAndCommandFields(tpl.getHexProtocolFields(), c.getFields());
            TcpHexLtvRepeatingConfig ml = c.getLtvRepeating() != null ? c.getLtvRepeating() : tpl.getHexLtvRepeating();
            boolean hasFields = mf != null && !mf.isEmpty();
            if (!hasFields && ml == null) {
                throw new IllegalArgumentException(
                        "Uplink/BOTH command needs fields or LTV (template or command override): " + c.getName());
            }
        }
    }
}
