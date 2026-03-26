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
        /** 默认不按键选协议，仅靠同步头/命令自动匹配；需要显式指定时再填写 protocolIdKey */
        c.setProtocolIdKey("");
        c.setResultObjectKey("parsed");
        c.setOutputKeyPrefix("");
        List<HexFrameTemplate> frames = new ArrayList<>();

        // 无同步字头：设备类型(1) + 功能码(1) + 数据长度 UINT16_LE(2)
        HexFrameTemplate deviceFrame = new HexFrameTemplate();
        deviceFrame.setId("device_response_frame");
        deviceFrame.setSyncHex("");
        deviceFrame.setSyncOffset(0);
        deviceFrame.setMinBytes(6);
        deviceFrame.setParamStartOffset(4);
        deviceFrame.setParamLenFieldOffset(2);
        deviceFrame.setCommandMatchOffset(1);
        List<HexFieldDefinition> deviceHeader = new ArrayList<>();
        addField(deviceHeader, "deviceType", 0, "UINT8");
        addField(deviceHeader, "functionCode", 1, "UINT8");
        addField(deviceHeader, "dataLen", 2, "UINT16_LE");
        deviceFrame.setHeaderFields(deviceHeader);
        frames.add(deviceFrame);

        // A5 5A + 源/目的/命令/参数长度 + 参数区
        HexFrameTemplate jammerFrame = new HexFrameTemplate();
        jammerFrame.setId("jammer_a55a_frame");
        jammerFrame.setSyncHex("A55A");
        jammerFrame.setSyncOffset(0);
        jammerFrame.setMinBytes(8);
        jammerFrame.setParamLenFieldOffset(5);
        jammerFrame.setParamStartOffset(7);
        jammerFrame.setCommandMatchOffset(4);
        List<HexFieldDefinition> jammerHeader = new ArrayList<>();
        addField(jammerHeader, "srcAddr", 2, "UINT8");
        addField(jammerHeader, "dstAddr", 3, "UINT8");
        addField(jammerHeader, "command", 4, "UINT8");
        addField(jammerHeader, "paramLen", 5, "UINT16_LE");
        jammerFrame.setHeaderFields(jammerHeader);
        frames.add(jammerFrame);

        // 监控 UDP：无同步头；包头 packetLen/deviceId/category/commandNumber 均为 UINT32_LE，参数区从 16 起
        HexFrameTemplate monitorUdp = new HexFrameTemplate();
        monitorUdp.setId("monitor_udp_frame");
        monitorUdp.setSyncHex("");
        monitorUdp.setSyncOffset(0);
        monitorUdp.setMinBytes(16);
        monitorUdp.setParamStartOffset(16);
        monitorUdp.setParamLenFieldOffset(0);
        monitorUdp.setCommandMatchOffset(12);
        List<HexFieldDefinition> monitorHeader = new ArrayList<>();
        addField(monitorHeader, "packetLen", 0, "UINT32_LE");
        addField(monitorHeader, "deviceId", 4, "UINT32_LE");
        addField(monitorHeader, "category", 8, "UINT32_LE");
        addField(monitorHeader, "commandNumber", 12, "UINT32_LE");
        monitorUdp.setHeaderFields(monitorHeader);
        frames.add(monitorUdp);

        c.setFrameTemplates(frames);

        List<HexProtocolDefinition> list = new ArrayList<>();

        // 表格 6-3：起始码 A5 5A，命令 0xA2，参数区首字节 0x41 + 20 字节数据，末字节累加和
        HexProtocolDefinition table63 = new HexProtocolDefinition();
        table63.setId("table63_cmdA2");
        table63.setTemplateId("jammer_a55a_frame");
        table63.setMinBytes(29);
        table63.setCommandByteOffset(4);
        table63.setCommandValue(0xA2);
        table63.setChecksum(sum8FromAddrToChecksum());
        List<HexFieldDefinition> f63 = new ArrayList<>();
        addField(f63, "subType", 0, "UINT8");
        HexFieldDefinition param1 = new HexFieldDefinition();
        param1.setName("param1Hex");
        param1.setOffset(1);
        param1.setType("HEX_SLICE");
        param1.setLength(20);
        f63.add(param1);
        table63.setFields(f63);
        list.add(table63);

        // 干扰器：命令 0x21，1 字节参数 + SUM8
        HexProtocolDefinition jammer21 = new HexProtocolDefinition();
        jammer21.setId("jammer_cmd_0x21");
        jammer21.setTemplateId("jammer_a55a_frame");
        jammer21.setMinBytes(9);
        jammer21.setCommandByteOffset(4);
        jammer21.setCommandValue(0x21);
        jammer21.setChecksum(sum8FromAddrToChecksum());
        List<HexFieldDefinition> fj21 = new ArrayList<>();
        addField(fj21, "paramByte0", 0, "UINT8");
        jammer21.setFields(fj21);
        list.add(jammer21);

        // 干扰器：命令 0x06 查云台，零长度参数 + SUM8
        HexProtocolDefinition jammer06 = new HexProtocolDefinition();
        jammer06.setId("jammer_cmd_0x06");
        jammer06.setTemplateId("jammer_a55a_frame");
        jammer06.setMinBytes(8);
        jammer06.setCommandByteOffset(4);
        jammer06.setCommandValue(0x06);
        jammer06.setChecksum(sum8FromAddrToChecksum());
        jammer06.setFields(new ArrayList<>());
        list.add(jammer06);

        // 监控 UDP：命令号为 offset 12 的 uint32 LE；子命令体为参数区至帧尾（示例命令 1/2/3/5）
        for (int cmd : new int[]{1, 2, 3, 5}) {
            HexProtocolDefinition mon = new HexProtocolDefinition();
            mon.setId("monitor_udp_cmd_" + cmd);
            mon.setTemplateId("monitor_udp_frame");
            mon.setMinBytes(16);
            mon.setCommandByteOffset(12);
            mon.setCommandValue(cmd);
            mon.setCommandMatchWidth(4);
            HexChecksumDefinition none = new HexChecksumDefinition();
            none.setType("NONE");
            mon.setChecksum(none);
            List<HexFieldDefinition> mf = new ArrayList<>();
            HexFieldDefinition body = new HexFieldDefinition();
            body.setName("subCommandBodyHex");
            body.setOffset(0);
            body.setType("HEX_SLICE");
            mf.add(body);
            mon.setFields(mf);
            list.add(mon);
        }

        // 响应写命令帧体：功能码 0x10，参数区为连续字节（长度=dataLen）
        HexProtocolDefinition writeResp = new HexProtocolDefinition();
        writeResp.setId("resp_write_cmd_0x10");
        writeResp.setTemplateId("device_response_frame");
        writeResp.setMinBytes(6);
        writeResp.setCommandByteOffset(1);
        writeResp.setCommandValue(0x10);
        writeResp.setChecksum(crc16CcittEnd());
        List<HexFieldDefinition> fw = new ArrayList<>();
        HexFieldDefinition paramBlock = new HexFieldDefinition();
        paramBlock.setName("paramSectionHex");
        paramBlock.setOffset(0);
        paramBlock.setType("HEX_SLICE_LEN_U16LE");
        fw.add(paramBlock);
        writeResp.setFields(fw);
        list.add(writeResp);

        // 响应读命令帧体：功能码 0x03，参数区为 TLV（参数序号 2 字节 + 长度 1 字节 + 值）
        HexProtocolDefinition readResp = new HexProtocolDefinition();
        readResp.setId("resp_read_cmd_0x03");
        readResp.setTemplateId("device_response_frame");
        readResp.setMinBytes(6);
        readResp.setCommandByteOffset(1);
        readResp.setCommandValue(0x03);
        readResp.setChecksum(crc16CcittEnd());
        List<HexFieldDefinition> fr = new ArrayList<>();
        HexFieldDefinition tlv = new HexFieldDefinition();
        tlv.setName("params");
        tlv.setOffset(0);
        tlv.setType("TLV_LIST");
        tlv.setToOffsetExclusive(-2);
        tlv.setTlvIdSize(2);
        tlv.setTlvIdEndian("BE");
        fr.add(tlv);
        readResp.setFields(fr);
        list.add(readResp);

        c.setProtocols(list);
        return c;
    }

    private static void addField(List<HexFieldDefinition> list, String name, int off, String type) {
        HexFieldDefinition f = new HexFieldDefinition();
        f.setName(name);
        f.setOffset(off);
        f.setType(type);
        list.add(f);
    }

    /** SUM8：从源地址到校验前一字节累加，校验在末字节 */
    private static HexChecksumDefinition sum8FromAddrToChecksum() {
        HexChecksumDefinition sum = new HexChecksumDefinition();
        sum.setType("SUM8");
        sum.setFromByte(2);
        sum.setToExclusive(-1);
        sum.setChecksumByteIndex(-1);
        return sum;
    }

    /** CRC16/CCITT-FALSE，2 字节大端在帧尾 */
    private static HexChecksumDefinition crc16CcittEnd() {
        HexChecksumDefinition cs = new HexChecksumDefinition();
        cs.setType("CRC16_CCITT");
        cs.setFromByte(0);
        cs.setToExclusive(-2);
        cs.setChecksumByteIndex(-2);
        return cs;
    }

}
