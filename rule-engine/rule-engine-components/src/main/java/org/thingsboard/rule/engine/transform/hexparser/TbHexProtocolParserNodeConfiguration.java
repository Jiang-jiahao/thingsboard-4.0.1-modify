/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.rule.engine.transform.hexparser;

import lombok.Data;
import org.thingsboard.rule.engine.api.NodeConfiguration;

import java.util.ArrayList;
import java.util.List;

@Data
public class TbHexProtocolParserNodeConfiguration implements NodeConfiguration<TbHexProtocolParserNodeConfiguration> {

    /** Telemetry / JSON key holding continuous hex string (no spaces) */
    private String hexInputKey;
    /** If set, message must contain this key to select protocol by id; if empty, sync prefix only */
    private String protocolIdKey;
    /** Place parser output under this object key; empty = merge to root with prefix */
    private String resultObjectKey;
    /** Prefix for each output field when merging to root */
    private String outputKeyPrefix;
    /** Protocol definitions */
    private List<HexProtocolDefinition> protocols;
    /** Reusable frame header (sync, length, checksum); referenced by {@link HexProtocolDefinition#getTemplateId()} */
    private List<HexFrameTemplate> frameTemplates;

    @Override
    public TbHexProtocolParserNodeConfiguration defaultConfiguration() {
        TbHexProtocolParserNodeConfiguration c = new TbHexProtocolParserNodeConfiguration();
        c.setHexInputKey("rawHex");
        c.setProtocolIdKey("");
        c.setResultObjectKey("parsed");
        c.setOutputKeyPrefix("");

        /*
         * 帧模板：无同步头。包头固定 16 字节 UINT32 LE：
         * 长度(0) 设备ID(4) 设备类别(8) 命令编号(12)；子命令体从字节 16 起。
         */
        HexFrameTemplate frame = new HexFrameTemplate();
        frame.setId("device_datagram_frame");
        frame.setSyncHex("");
        frame.setSyncOffset(0);
        frame.setMinBytes(16);
        frame.setParamStartOffset(16);
        frame.setParamLenFieldOffset(0);
        frame.setCommandMatchOffset(12);
        List<HexFieldDefinition> header = new ArrayList<>();
        addField(header, "packetLen", 0, "UINT32_LE");
        addField(header, "deviceId", 4, "UINT32_LE");
        addField(header, "category", 8, "UINT32_LE");
        addField(header, "commandNumber", 12, "UINT32_LE");
        frame.setHeaderFields(header);

        /*
         * 子命令体（参数区）在帧模板上定义，偏移相对 paramStartOffset；协议行只负责命令匹配（标识）。
         * 示例：总长 + 编号 STRUCT + 信息单元 UNIT_LIST（长度+编号+数据）。
         */
        List<HexFieldDefinition> payload = new ArrayList<>();
        addField(payload, "subCommandPayloadLen", 0, "UINT32_LE");

        HexFieldDefinition numberBlock = new HexFieldDefinition();
        numberBlock.setName("numberBlock");
        numberBlock.setOffset(4);
        numberBlock.setType("STRUCT");
        numberBlock.setLength(4);
        numberBlock.setNestedFields(bianHaoNestedFields());
        payload.add(numberBlock);

        HexFieldDefinition dataItems = new HexFieldDefinition();
        dataItems.setName("dataItems");
        dataItems.setOffset(8);
        dataItems.setType("UNIT_LIST");
        dataItems.setUnitLengthFieldOffset(0);
        dataItems.setUnitIdByteLength(4);
        dataItems.setUnitDataOutputName("dataContentHex");
        dataItems.setUnitNumberOutputName("itemNumber");
        dataItems.setListItemFields(bianHaoNestedFields());
        payload.add(dataItems);

        frame.setPayloadFields(payload);

        c.setFrameTemplates(List.of(frame));

        HexProtocolDefinition proto = new HexProtocolDefinition();
        proto.setId("device_datagram_subcmd");
        proto.setTemplateId("device_datagram_frame");
        proto.setMinBytes(16);
        proto.setCommandByteOffset(12);
        proto.setCommandValue(1);
        proto.setCommandMatchWidth(4);
        HexChecksumDefinition none = new HexChecksumDefinition();
        none.setType("NONE");
        proto.setChecksum(none);
        proto.setFields(new ArrayList<>());
        c.setProtocols(List.of(proto));
        return c;
    }

    /** 编号区：模块字段(0-1) 当前模块编号(2) 模块类型(3)，偏移相对编号段起点 */
    private static List<HexFieldDefinition> bianHaoNestedFields() {
        List<HexFieldDefinition> n = new ArrayList<>();
        addField(n, "moduleField", 0, "UINT16_LE");
        addField(n, "currentModuleNo", 2, "UINT8");
        addField(n, "moduleType", 3, "UINT8");
        return n;
    }

    private static void addField(List<HexFieldDefinition> list, String name, int off, String type) {
        HexFieldDefinition f = new HexFieldDefinition();
        f.setName(name);
        f.setOffset(off);
        f.setType(type);
        list.add(f);
    }

}
