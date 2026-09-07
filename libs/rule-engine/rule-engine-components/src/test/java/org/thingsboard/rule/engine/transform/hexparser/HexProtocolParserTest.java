/**
 * Copyright © 2016-2025 The Thingsboard Authors
 */
package org.thingsboard.rule.engine.transform.hexparser;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HexProtocolParserTest {

    private static List<HexFrameTemplate> jammerFrameTemplates() {
        HexFrameTemplate jammerFrame = new HexFrameTemplate();
        jammerFrame.setId("jammer_a55a_frame");
        jammerFrame.setSyncHex("A55A");
        jammerFrame.setSyncOffset(0);
        jammerFrame.setMinBytes(8);
        jammerFrame.setParamLenFieldOffset(5);
        jammerFrame.setParamStartOffset(7);
        jammerFrame.setCommandMatchOffset(4);
        List<HexFieldDefinition> jammerHeader = new ArrayList<>();
        addHeaderField(jammerHeader, "srcAddr", 2, "UINT8");
        addHeaderField(jammerHeader, "dstAddr", 3, "UINT8");
        addHeaderField(jammerHeader, "command", 4, "UINT8");
        addHeaderField(jammerHeader, "paramLen", 5, "UINT16_LE");
        jammerFrame.setHeaderFields(jammerHeader);
        return List.of(jammerFrame);
    }

    private static List<HexProtocolDefinition> jammerTable63Protocols() {
        List<HexProtocolDefinition> list = new ArrayList<>();
        HexProtocolDefinition table63 = new HexProtocolDefinition();
        table63.setId("table63_cmdA2");
        table63.setTemplateId("jammer_a55a_frame");
        table63.setMinBytes(29);
        table63.setCommandByteOffset(4);
        table63.setCommandValue(0xA2);
        table63.setChecksum(sum8Jammer());
        List<HexFieldDefinition> f63 = new ArrayList<>();
        addHeaderField(f63, "subType", 0, "UINT8");
        HexFieldDefinition param1 = new HexFieldDefinition();
        param1.setName("param1Hex");
        param1.setOffset(1);
        param1.setType("HEX_SLICE");
        param1.setLength(20);
        f63.add(param1);
        table63.setFields(f63);
        list.add(table63);
        HexProtocolDefinition jammer21 = new HexProtocolDefinition();
        jammer21.setId("jammer_cmd_0x21");
        jammer21.setTemplateId("jammer_a55a_frame");
        jammer21.setMinBytes(9);
        jammer21.setCommandByteOffset(4);
        jammer21.setCommandValue(0x21);
        jammer21.setChecksum(sum8Jammer());
        jammer21.setFields(List.of(simpleField("paramByte0", 0, "UINT8")));
        list.add(jammer21);
        return list;
    }

    private static void addHeaderField(List<HexFieldDefinition> list, String name, int off, String type) {
        HexFieldDefinition f = new HexFieldDefinition();
        f.setName(name);
        f.setOffset(off);
        f.setType(type);
        list.add(f);
    }

    private static HexFieldDefinition simpleField(String name, int off, String type) {
        HexFieldDefinition f = new HexFieldDefinition();
        f.setName(name);
        f.setOffset(off);
        f.setType(type);
        return f;
    }

    private static HexChecksumDefinition sum8Jammer() {
        HexChecksumDefinition sum = new HexChecksumDefinition();
        sum.setType("SUM8");
        sum.setFromByte(2);
        sum.setToExclusive(-1);
        sum.setChecksumByteIndex(-1);
        return sum;
    }

    @Test
    public void findProtocolWhenProtocolIdWrongFallsBackToAutoMatch() {
        byte[] jammer21 = new byte[]{
                (byte) 0xA5, 0x5A, 0x10, 0x10, 0x21, 0x01, 0x00, 0x55, (byte) 0x97
        };
        HexProtocolDefinition d = HexProtocolParser.findProtocol(jammerTable63Protocols(), "table63_cmdA2", jammer21,
                jammerFrameTemplates());
        Assertions.assertEquals("jammer_cmd_0x21", d.getId());
    }

    @Test
    public void findProtocolSelectsByCommandByte() {
        List<HexProtocolDefinition> list = new ArrayList<>();
        HexProtocolDefinition p21 = new HexProtocolDefinition();
        p21.setId("jammer_cmd_21");
        p21.setSyncHex("A55A");
        p21.setSyncOffset(0);
        p21.setMinBytes(6);
        p21.setCommandByteOffset(4);
        p21.setCommandValue(0x21);
        list.add(p21);
        HexProtocolDefinition pa2 = new HexProtocolDefinition();
        pa2.setId("jammer_cmd_a2");
        pa2.setSyncHex("A55A");
        pa2.setSyncOffset(0);
        pa2.setMinBytes(6);
        pa2.setCommandByteOffset(4);
        pa2.setCommandValue(0xA2);
        list.add(pa2);
        byte[] frame21 = new byte[]{(byte) 0xA5, 0x5A, 0x10, (byte) 0xA0, 0x21};
        byte[] frameA2 = new byte[]{(byte) 0xA5, 0x5A, (byte) 0xA0, 0x10, (byte) 0xA2};
        Assertions.assertEquals("jammer_cmd_21", HexProtocolParser.findProtocol(list, "", frame21).getId());
        Assertions.assertEquals("jammer_cmd_a2", HexProtocolParser.findProtocol(list, "", frameA2).getId());
    }

    @Test
    public void findProtocolResolvesSyncFromFrameTemplate() {
        byte[] buf = new byte[29];
        buf[0] = (byte) 0xA5;
        buf[1] = 0x5A;
        buf[2] = 0x10;
        buf[3] = 0x10;
        buf[4] = (byte) 0xA2;
        buf[5] = 0x15;
        buf[6] = 0x00;
        buf[7] = 0x41;
        int sum = 0;
        for (int i = 2; i <= 27; i++) {
            sum = (sum + (buf[i] & 0xFF)) & 0xFF;
        }
        buf[28] = (byte) sum;
        HexProtocolDefinition d = HexProtocolParser.findProtocol(jammerTable63Protocols(), "", buf, jammerFrameTemplates());
        Assertions.assertEquals("table63_cmdA2", d.getId());
    }

    @Test
    public void expandCopiesCommandMatchWidth() {
        HexFrameTemplate t = new HexFrameTemplate();
        t.setId("t_mon");
        t.setSyncHex("");
        t.setSyncOffset(0);
        t.setMinBytes(16);
        t.setParamStartOffset(16);
        t.setCommandMatchOffset(12);
        t.setHeaderFields(new ArrayList<>());
        List<HexFrameTemplate> templates = new ArrayList<>();
        templates.add(t);
        HexProtocolDefinition raw = new HexProtocolDefinition();
        raw.setId("v_mon");
        raw.setTemplateId("t_mon");
        raw.setCommandMatchWidth(4);
        raw.setFields(new ArrayList<>());
        HexProtocolDefinition out = HexProtocolExpander.expand(raw, templates);
        Assertions.assertEquals(4, out.getCommandMatchWidth().intValue());
    }

    @Test
    public void expandUsesCustomHeaderFieldsFromTemplate() {
        HexFrameTemplate t = new HexFrameTemplate();
        t.setId("t_custom");
        t.setSyncHex("A55A");
        t.setSyncOffset(0);
        t.setParamStartOffset(7);
        t.setParamLenFieldOffset(5);
        List<HexFieldDefinition> hf = new ArrayList<>();
        HexFieldDefinition f = new HexFieldDefinition();
        f.setName("deviceId");
        f.setOffset(12);
        f.setType("UINT8");
        hf.add(f);
        t.setHeaderFields(hf);
        List<HexFrameTemplate> templates = new ArrayList<>();
        templates.add(t);
        HexProtocolDefinition raw = new HexProtocolDefinition();
        raw.setId("v1");
        raw.setTemplateId("t_custom");
        raw.setFields(new ArrayList<>());
        HexProtocolDefinition out = HexProtocolExpander.expand(raw, templates);
        Assertions.assertEquals(1, out.getFields().size());
        Assertions.assertEquals("deviceId", out.getFields().get(0).getName());
        Assertions.assertEquals(12, out.getFields().get(0).getOffset());
    }

    @Test
    public void defaultConfigurationParsesDeviceDatagramLayout() {
        TbHexProtocolParserNodeConfiguration cfg = new TbHexProtocolParserNodeConfiguration().defaultConfiguration();
        HexProtocolDefinition def = HexProtocolExpander.expand(cfg.getProtocols().get(0), cfg.getFrameTemplates());
        // 34 bytes: 16 header + subLen(4) + numberBlock(4) + unit(uint32 6 + id4 + data2)
        byte[] buf = new byte[34];
        buf[0] = 34;
        buf[1] = 0;
        buf[2] = 0;
        buf[3] = 0;
        buf[4] = 0x11;
        buf[5] = 0;
        buf[6] = 0;
        buf[7] = 0;
        buf[8] = 0;
        buf[9] = 0;
        buf[10] = 0;
        buf[11] = 0;
        buf[12] = 1;
        buf[13] = 0;
        buf[14] = 0;
        buf[15] = 0;
        buf[16] = 14;
        buf[17] = 0;
        buf[18] = 0;
        buf[19] = 0;
        buf[20] = 0x12;
        buf[21] = 0x34;
        buf[22] = 5;
        buf[23] = 6;
        buf[24] = 6;
        buf[25] = 0;
        buf[26] = 0;
        buf[27] = 0;
        buf[28] = 1;
        buf[29] = 0;
        buf[30] = 3;
        buf[31] = 4;
        buf[32] = (byte) 0xDE;
        buf[33] = (byte) 0xFA;
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals("monitor_udp_datagram", out.get("protocolId").asText());
        Assertions.assertEquals(14, out.get("subCommandPayloadLen").asInt());
        Assertions.assertEquals(0x3412, out.get("numberBlock").get("moduleField").asInt());
        Assertions.assertEquals(5, out.get("numberBlock").get("currentModuleNo").asInt());
        Assertions.assertEquals(6, out.get("numberBlock").get("moduleType").asInt());
        Assertions.assertEquals(1, out.get("dataItems").size());
        Assertions.assertEquals(6, out.get("dataItems").get(0).get("payloadLength").asInt());
        Assertions.assertEquals(1, out.get("dataItems").get(0).get("itemNumber").get("moduleField").asInt());
        Assertions.assertEquals("DEFA", out.get("dataItems").get(0).get("dataContentHex").asText());
    }

    @Test
    public void findProtocolHeadlessMatchesU32CommandAtOffset12() {
        HexFrameTemplate t = new HexFrameTemplate();
        t.setId("mon_udp");
        t.setSyncHex("");
        t.setSyncOffset(0);
        t.setMinBytes(16);
        t.setParamStartOffset(16);
        t.setParamLenFieldOffset(0);
        t.setCommandMatchOffset(12);
        List<HexFieldDefinition> mh = new ArrayList<>();
        addHeaderField(mh, "packetLen", 0, "UINT32_LE");
        addHeaderField(mh, "deviceId", 4, "UINT32_LE");
        addHeaderField(mh, "category", 8, "UINT32_LE");
        addHeaderField(mh, "commandNumber", 12, "UINT32_LE");
        t.setHeaderFields(mh);
        HexProtocolDefinition p = new HexProtocolDefinition();
        p.setId("mon_cmd_2");
        p.setTemplateId("mon_udp");
        p.setMinBytes(16);
        p.setCommandByteOffset(12);
        p.setCommandValue(2);
        p.setCommandMatchWidth(4);
        HexChecksumDefinition none = new HexChecksumDefinition();
        none.setType("NONE");
        p.setChecksum(none);
        HexFieldDefinition body = new HexFieldDefinition();
        body.setName("subCommandBodyHex");
        body.setOffset(0);
        body.setType("HEX_SLICE");
        p.setFields(List.of(body));
        byte[] buf = new byte[20];
        buf[0] = 0x14;
        buf[1] = 0x00;
        buf[2] = 0x00;
        buf[3] = 0x00;
        buf[4] = 0x01;
        buf[12] = 0x02;
        buf[16] = (byte) 0xAA;
        buf[17] = (byte) 0xBB;
        buf[18] = (byte) 0xCC;
        buf[19] = (byte) 0xDD;
        HexProtocolDefinition d = HexProtocolParser.findProtocol(List.of(p), "", buf, List.of(t));
        Assertions.assertEquals("mon_cmd_2", d.getId());
        HexProtocolDefinition def = HexProtocolExpander.expand(d, List.of(t));
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals("AABBCCDD", out.get("subCommandBodyHex").asText());
    }

    @Test
    public void jammerCmd0x21ParsesParamByte() {
        byte[] buf = new byte[]{
                (byte) 0xA5, 0x5A, 0x10, 0x10, 0x21, 0x01, 0x00, 0x55, (byte) 0x97
        };
        HexProtocolDefinition d = HexProtocolParser.findProtocol(jammerTable63Protocols(), "", buf, jammerFrameTemplates());
        Assertions.assertEquals("jammer_cmd_0x21", d.getId());
        HexProtocolDefinition def = HexProtocolExpander.expand(d, jammerFrameTemplates());
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals(0x55, out.get("paramByte0").asInt());
    }

    @Test
    public void findProtocolHeadlessMatchesFunctionCode() {
        HexFrameTemplate devFrame = new HexFrameTemplate();
        devFrame.setId("device_response_frame");
        devFrame.setSyncHex("");
        devFrame.setSyncOffset(0);
        devFrame.setMinBytes(6);
        devFrame.setParamStartOffset(4);
        devFrame.setParamLenFieldOffset(2);
        devFrame.setCommandMatchOffset(1);
        List<HexFieldDefinition> dh = new ArrayList<>();
        addHeaderField(dh, "deviceType", 0, "UINT8");
        addHeaderField(dh, "functionCode", 1, "UINT8");
        addHeaderField(dh, "dataLen", 2, "UINT16_LE");
        devFrame.setHeaderFields(dh);
        List<HexFrameTemplate> tpl = List.of(devFrame);

        HexProtocolDefinition readResp = new HexProtocolDefinition();
        readResp.setId("resp_read_cmd_0x03");
        readResp.setTemplateId("device_response_frame");
        readResp.setMinBytes(6);
        readResp.setCommandByteOffset(1);
        readResp.setCommandValue(0x03);
        readResp.setChecksum(crc16CcittEnd());
        HexFieldDefinition tlv = new HexFieldDefinition();
        tlv.setName("params");
        tlv.setOffset(0);
        tlv.setType("TLV_LIST");
        tlv.setToOffsetExclusive(-2);
        tlv.setTlvIdSize(2);
        tlv.setTlvIdEndian("BE");
        readResp.setFields(List.of(tlv));

        HexProtocolDefinition writeResp = new HexProtocolDefinition();
        writeResp.setId("resp_write_cmd_0x10");
        writeResp.setTemplateId("device_response_frame");
        writeResp.setMinBytes(6);
        writeResp.setCommandByteOffset(1);
        writeResp.setCommandValue(0x10);
        writeResp.setChecksum(crc16CcittEnd());
        HexFieldDefinition paramBlock = new HexFieldDefinition();
        paramBlock.setName("paramSectionHex");
        paramBlock.setOffset(0);
        paramBlock.setType("HEX_SLICE_LEN_U16LE");
        writeResp.setFields(List.of(paramBlock));

        List<HexProtocolDefinition> protos = List.of(readResp, writeResp);

        byte[] read = new byte[10];
        read[0] = 0x01;
        read[1] = 0x03;
        read[2] = 0x04;
        read[3] = 0x00;
        read[4] = 0x00;
        read[5] = 0x01;
        read[6] = 0x01;
        read[7] = (byte) 0xAA;
        int cr = HexProtocolParser.crc16Ccitt(read, 0, 8);
        read[8] = (byte) ((cr >> 8) & 0xFF);
        read[9] = (byte) (cr & 0xFF);
        Assertions.assertEquals("resp_read_cmd_0x03",
                HexProtocolParser.findProtocol(protos, "", read, tpl).getId());

        byte[] write = new byte[8];
        write[0] = 0x01;
        write[1] = 0x10;
        write[2] = 0x02;
        write[3] = 0x00;
        write[4] = 0x11;
        write[5] = 0x22;
        int cw = HexProtocolParser.crc16Ccitt(write, 0, 6);
        write[6] = (byte) ((cw >> 8) & 0xFF);
        write[7] = (byte) (cw & 0xFF);
        Assertions.assertEquals("resp_write_cmd_0x10",
                HexProtocolParser.findProtocol(protos, "", write, tpl).getId());
    }

    private static HexChecksumDefinition crc16CcittEnd() {
        HexChecksumDefinition cs = new HexChecksumDefinition();
        cs.setType("CRC16_CCITT");
        cs.setFromByte(0);
        cs.setToExclusive(-2);
        cs.setChecksumByteIndex(-2);
        return cs;
    }

    @Test
    public void listItemFieldsSameLayoutForEveryTlvEntry() {
        HexProtocolDefinition def = new HexProtocolDefinition();
        def.setId("tlv_flat");
        def.setMinBytes(8);
        HexFieldDefinition tlv = new HexFieldDefinition();
        tlv.setName("params");
        tlv.setOffset(0);
        tlv.setType("TLV_LIST");
        tlv.setToOffsetExclusive(8);
        tlv.setTlvIdSize(2);
        tlv.setTlvIdEndian("BE");
        HexFieldDefinition inner = new HexFieldDefinition();
        inner.setName("byte0");
        inner.setOffset(0);
        inner.setType("UINT8");
        tlv.setListItemFields(List.of(inner));
        def.setFields(Collections.singletonList(tlv));
        // id=1 len=1 data=0xAA ; id=2 len=1 data=0xBB
        byte[] buf = new byte[]{0x00, 0x01, 0x01, (byte) 0xAA, 0x00, 0x02, 0x01, (byte) 0xBB};
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals(2, out.get("params").size());
        ObjectNode first = (ObjectNode) out.get("params").get(0);
        Assertions.assertEquals(0xAA, first.get("nested").get("byte0").asInt());
        ObjectNode second = (ObjectNode) out.get("params").get(1);
        Assertions.assertEquals(0xBB, second.get("nested").get("byte0").asInt());
    }

    @Test
    public void unitListParsesUint32LengthIdAndData() {
        HexProtocolDefinition def = new HexProtocolDefinition();
        def.setId("unit_list_demo");
        def.setMinBytes(18);
        HexFieldDefinition ul = new HexFieldDefinition();
        ul.setName("infoUnits");
        ul.setOffset(0);
        ul.setType("UNIT_LIST");
        ul.setToOffsetExclusive(18);
        ul.setUnitIdByteLength(4);
        HexFieldDefinition mod = new HexFieldDefinition();
        mod.setName("moduleField");
        mod.setOffset(0);
        mod.setType("UINT16_LE");
        HexFieldDefinition cur = new HexFieldDefinition();
        cur.setName("currentModuleNo");
        cur.setOffset(2);
        cur.setType("UINT8");
        HexFieldDefinition typ = new HexFieldDefinition();
        typ.setName("moduleType");
        typ.setOffset(3);
        typ.setType("UINT8");
        ul.setListItemFields(List.of(mod, cur, typ));
        def.setFields(Collections.singletonList(ul));
        // unit1: payloadLen=6, id 01 02 03 04, data AA BB
        // unit2: payloadLen=4, id 11 12 13 14, data (none)
        byte[] buf = new byte[]{
                0x06, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, (byte) 0xAA, (byte) 0xBB,
                0x04, 0x00, 0x00, 0x00, 0x11, 0x12, 0x13, 0x14
        };
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals(2, out.get("infoUnits").size());
        ObjectNode u0 = (ObjectNode) out.get("infoUnits").get(0);
        Assertions.assertEquals(6, u0.get("payloadLength").asInt());
        Assertions.assertEquals("AABB", u0.get("dataContentHex").asText());
        ObjectNode num0 = (ObjectNode) u0.get("number");
        Assertions.assertEquals(0x0201, num0.get("moduleField").asInt());
        Assertions.assertEquals(3, num0.get("currentModuleNo").asInt());
        Assertions.assertEquals(4, num0.get("moduleType").asInt());
        ObjectNode u1 = (ObjectNode) out.get("infoUnits").get(1);
        Assertions.assertEquals(4, u1.get("payloadLength").asInt());
        Assertions.assertEquals("", u1.get("dataContentHex").asText());
        ObjectNode num1 = (ObjectNode) u1.get("number");
        Assertions.assertEquals(0x1211, num1.get("moduleField").asInt());
    }

    @Test
    public void tlvListParsesNestedTemplateByParamId() {
        HexProtocolDefinition def = new HexProtocolDefinition();
        def.setId("tlv_nested");
        def.setMinBytes(5);
        HexFieldDefinition tlv = new HexFieldDefinition();
        tlv.setName("params");
        tlv.setOffset(0);
        tlv.setType("TLV_LIST");
        tlv.setToOffsetExclusive(5);
        tlv.setTlvIdSize(2);
        tlv.setTlvIdEndian("BE");
        TlvNestedRule rule = new TlvNestedRule();
        rule.setParamId(1);
        HexFieldDefinition innerA = new HexFieldDefinition();
        innerA.setName("innerA");
        innerA.setOffset(0);
        innerA.setType("UINT8");
        HexFieldDefinition innerB = new HexFieldDefinition();
        innerB.setName("innerB");
        innerB.setOffset(1);
        innerB.setType("UINT8");
        rule.setFields(List.of(innerA, innerB));
        tlv.setTlvNestedRules(Collections.singletonList(rule));
        def.setFields(Collections.singletonList(tlv));
        byte[] buf = new byte[]{0x00, 0x01, 0x02, (byte) 0xAA, (byte) 0xBB};
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertTrue(out.get("params").isArray());
        Assertions.assertEquals(1, out.get("params").size());
        ObjectNode first = (ObjectNode) out.get("params").get(0);
        Assertions.assertEquals(1, first.get("paramId").asInt());
        Assertions.assertEquals(2, first.get("len").asInt());
        Assertions.assertTrue(first.has("nested"));
        Assertions.assertEquals(0xAA, first.get("nested").get("innerA").asInt());
        Assertions.assertEquals(0xBB, first.get("nested").get("innerB").asInt());
    }

    @Test
    public void crc16CcittStandardString() {
        byte[] data = "123456789".getBytes(StandardCharsets.US_ASCII);
        Assertions.assertEquals(0x29B1, HexProtocolParser.crc16Ccitt(data, 0, data.length));
    }

    @Test
    public void crc16CcittChecksumValidates() {
        HexProtocolDefinition def = new HexProtocolDefinition();
        def.setId("ccitt");
        byte[] payload = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        int crc = HexProtocolParser.crc16Ccitt(payload, 0, payload.length);
        byte[] buf = new byte[payload.length + 2];
        System.arraycopy(payload, 0, buf, 0, payload.length);
        buf[payload.length] = (byte) ((crc >> 8) & 0xFF);
        buf[payload.length + 1] = (byte) (crc & 0xFF);
        def.setMinBytes(buf.length);
        HexChecksumDefinition cs = new HexChecksumDefinition();
        cs.setType("CRC16_CCITT");
        cs.setFromByte(0);
        cs.setToExclusive(payload.length);
        cs.setChecksumByteIndex(payload.length);
        def.setChecksum(cs);
        HexFieldDefinition f = new HexFieldDefinition();
        f.setName("b0");
        f.setOffset(0);
        f.setType("UINT8");
        def.setFields(Collections.singletonList(f));
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals(1, out.get("b0").asInt());
    }

    @Test
    public void testFloat32Endian() {
        HexProtocolDefinition def = new HexProtocolDefinition();
        def.setId("float_test");
        def.setMinBytes(16);
        ArrayList<HexFieldDefinition> fields = new ArrayList<>();
        HexFieldDefinition le = new HexFieldDefinition();
        le.setName("vLe");
        le.setOffset(0);
        le.setType("FLOAT32_LE");
        fields.add(le);
        HexFieldDefinition be = new HexFieldDefinition();
        be.setName("vBe");
        be.setOffset(4);
        be.setType("FLOAT32_BE");
        fields.add(be);
        def.setFields(fields);
        // 1.0f IEEE754: LE 00 00 80 3F, BE 3F 80 00 00
        byte[] buf = new byte[]{
                0x00, 0x00, (byte) 0x80, 0x3F,
                0x3F, (byte) 0x80, 0x00, 0x00
        };
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals(1.0, out.get("vLe").asDouble(), 1e-9);
        Assertions.assertEquals(1.0, out.get("vBe").asDouble(), 1e-9);
    }

    @Test
    public void structParsesNestedOffsetsRelativeToSegment() {
        HexProtocolDefinition def = new HexProtocolDefinition();
        def.setId("struct_demo");
        def.setMinBytes(5);
        HexFieldDefinition st = new HexFieldDefinition();
        st.setName("body");
        st.setOffset(0);
        st.setType("STRUCT");
        st.setLength(5);
        HexFieldDefinition a = new HexFieldDefinition();
        a.setName("a");
        a.setOffset(0);
        a.setType("UINT8");
        HexFieldDefinition b = new HexFieldDefinition();
        b.setName("b");
        b.setOffset(1);
        b.setType("UINT16_LE");
        st.setNestedFields(List.of(a, b));
        def.setFields(List.of(st));
        byte[] buf = new byte[]{0x10, 0x34, 0x12, 0x00, 0x00};
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertTrue(out.get("body").isObject());
        Assertions.assertEquals(0x10, out.get("body").get("a").asInt());
        Assertions.assertEquals(0x1234, out.get("body").get("b").asInt());
    }

    @Test
    public void genericListUntilEndWithUint8Prefix() {
        HexProtocolDefinition def = new HexProtocolDefinition();
        def.setId("gen_list");
        def.setMinBytes(8);
        HexFieldDefinition gl = new HexFieldDefinition();
        gl.setName("items");
        gl.setOffset(0);
        gl.setType("GENERIC_LIST");
        gl.setToOffsetExclusive(8);
        gl.setListCountMode("UNTIL_END");
        gl.setListItemLengthMode("PREFIX_UINT8");
        HexFieldDefinition x = new HexFieldDefinition();
        x.setName("x");
        x.setOffset(0);
        x.setType("UINT8");
        gl.setListItemFields(List.of(x));
        def.setFields(List.of(gl));
        // 01 AA | 02 BB CC
        byte[] buf = new byte[]{0x01, (byte) 0xAA, 0x02, (byte) 0xBB, (byte) 0xCC, 0x00, 0x00, 0x00};
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals(2, out.get("items").size());
        Assertions.assertEquals(0xAA, out.get("items").get(0).get("x").asInt());
        Assertions.assertEquals(0xBB, out.get("items").get(1).get("x").asInt());
    }

    @Test
    public void genericListFixedCountAndFixedElementLength() {
        HexProtocolDefinition def = new HexProtocolDefinition();
        def.setId("gen_fixed");
        def.setMinBytes(6);
        HexFieldDefinition gl = new HexFieldDefinition();
        gl.setName("recs");
        gl.setOffset(0);
        gl.setType("GENERIC_LIST");
        gl.setToOffsetExclusive(6);
        gl.setListCountMode("FIXED");
        gl.setListCount(2);
        gl.setListItemLengthMode("FIXED");
        gl.setListItemFixedLength(3);
        HexFieldDefinition id = new HexFieldDefinition();
        id.setName("id");
        id.setOffset(0);
        id.setType("UINT8");
        HexFieldDefinition val = new HexFieldDefinition();
        val.setName("val");
        val.setOffset(1);
        val.setType("UINT16_BE");
        gl.setListItemFields(List.of(id, val));
        def.setFields(List.of(gl));
        byte[] buf = new byte[]{0x01, 0x00, 0x7B, 0x02, 0x01, (byte) 0xCD};
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals(2, out.get("recs").size());
        Assertions.assertEquals(1, out.get("recs").get(0).get("id").asInt());
        Assertions.assertEquals(0x007B, out.get("recs").get(0).get("val").asInt());
        Assertions.assertEquals(2, out.get("recs").get(1).get("id").asInt());
        Assertions.assertEquals(0x01CD, out.get("recs").get(1).get("val").asInt());
    }

    @Test
    public void genericListCountFromField() {
        HexProtocolDefinition def = new HexProtocolDefinition();
        def.setId("gen_from_field");
        def.setMinBytes(5);
        HexFieldDefinition gl = new HexFieldDefinition();
        gl.setName("rows");
        gl.setOffset(1);
        gl.setType("GENERIC_LIST");
        gl.setToOffsetExclusive(5);
        gl.setListCountMode("FROM_FIELD");
        gl.setListCountFieldOffset(0);
        gl.setListCountFieldType("UINT8");
        gl.setListItemLengthMode("FIXED");
        gl.setListItemFixedLength(1);
        HexFieldDefinition v = new HexFieldDefinition();
        v.setName("v");
        v.setOffset(0);
        v.setType("UINT8");
        gl.setListItemFields(List.of(v));
        def.setFields(List.of(gl));
        byte[] buf = new byte[]{0x02, 0x11, 0x22, 0x33, 0x44};
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals(2, out.get("rows").size());
        Assertions.assertEquals(0x11, out.get("rows").get(0).get("v").asInt());
        Assertions.assertEquals(0x22, out.get("rows").get(1).get("v").asInt());
    }

    @Test
    public void testFloat64Endian() {
        HexProtocolDefinition def = new HexProtocolDefinition();
        def.setId("double_test");
        def.setMinBytes(16);
        ArrayList<HexFieldDefinition> fields = new ArrayList<>();
        HexFieldDefinition le = new HexFieldDefinition();
        le.setName("dLe");
        le.setOffset(0);
        le.setType("FLOAT64_LE");
        fields.add(le);
        HexFieldDefinition be = new HexFieldDefinition();
        be.setName("dBe");
        be.setOffset(8);
        be.setType("FLOAT64_BE");
        fields.add(be);
        def.setFields(fields);
        // 1.0d: LE 00 00 00 00 00 00 F0 3F, BE 3F F0 00 00 00 00 00 00
        byte[] buf = new byte[16];
        buf[0] = 0x00;
        buf[1] = 0x00;
        buf[2] = 0x00;
        buf[3] = 0x00;
        buf[4] = 0x00;
        buf[5] = 0x00;
        buf[6] = (byte) 0xF0;
        buf[7] = 0x3F;
        buf[8] = 0x3F;
        buf[9] = (byte) 0xF0;
        buf[10] = 0x00;
        buf[11] = 0x00;
        buf[12] = 0x00;
        buf[13] = 0x00;
        buf[14] = 0x00;
        buf[15] = 0x00;
        ObjectNode out = HexProtocolParser.parse(def, buf);
        Assertions.assertEquals(1.0, out.get("dLe").asDouble(), 1e-15);
        Assertions.assertEquals(1.0, out.get("dBe").asDouble(), 1e-15);
    }

}
