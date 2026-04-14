/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.thingsboard.server.transport.tcp.util;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.data.device.profile.TcpHexCommandProfile;
import org.thingsboard.server.common.data.device.profile.TcpHexFieldDefinition;
import org.thingsboard.server.common.data.device.profile.TcpHexLtvChunkOrder;
import org.thingsboard.server.common.data.device.profile.TcpHexLtvRepeatingConfig;
import org.thingsboard.server.common.data.device.profile.TcpHexLtvTagMapping;
import org.thingsboard.server.common.data.device.profile.TcpHexUnknownTagMode;
import org.thingsboard.server.common.data.device.profile.TcpHexValueType;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TcpHexProtocolParserTest {

    @Test
    void parsesUint16BeAndFloatBe() {
        String hex = "006440480fdb";
        var payload = JsonParser.parseString("{\"hex\":\"" + hex + "\"}");
        TcpHexFieldDefinition a = new TcpHexFieldDefinition();
        a.setKey("count");
        a.setByteOffset(0);
        a.setValueType(TcpHexValueType.UINT16_BE);
        TcpHexFieldDefinition b = new TcpHexFieldDefinition();
        b.setKey("pi");
        b.setByteOffset(2);
        b.setValueType(TcpHexValueType.FLOAT_BE);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, null, List.of(a, b), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("count").getAsInt()).isEqualTo(100);
        assertThat(out.get().get("pi").getAsDouble()).isBetween(3.13, 3.15);
    }

    @Test
    void scaleAndMask() {
        String hex = "ff00";
        var payload = JsonParser.parseString("{\"hex\":\"" + hex + "\"}");
        TcpHexFieldDefinition a = new TcpHexFieldDefinition();
        a.setKey("lo");
        a.setByteOffset(0);
        a.setValueType(TcpHexValueType.UINT16_BE);
        a.setBitMask(0xFFL);
        a.setScale(0.1);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, null, List.of(a), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("lo").getAsDouble()).isEqualTo(0.0);
        a.setBitMask(0xFFFFL);
        out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, null, List.of(a), null, null, UUID.randomUUID());
        assertThat(out.get().get("lo").getAsDouble()).isCloseTo(6528.0, org.assertj.core.data.Offset.offset(1e-6));
    }

    @Test
    void bytesAsHexSlice() {
        var payload = JsonParser.parseString("{\"hex\":\"aabbccdd\"}");
        TcpHexFieldDefinition a = new TcpHexFieldDefinition();
        a.setKey("head");
        a.setByteOffset(0);
        a.setValueType(TcpHexValueType.BYTES_AS_HEX);
        a.setByteLength(2);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, null, List.of(a), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("head").getAsString()).isEqualTo("aabb");
    }

    @Test
    void fixedIntegralMatchEmitsTelemetry() {
        var payload = JsonParser.parseString("{\"hex\":\"a5\"}");
        TcpHexFieldDefinition a = new TcpHexFieldDefinition();
        a.setKey("h");
        a.setByteOffset(0);
        a.setValueType(TcpHexValueType.UINT8);
        a.setFixedWireIntegralValue(165L);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, null, List.of(a), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("h").getAsInt()).isEqualTo(165);
    }

    @Test
    void fixedIntegralMismatchSkipsFieldWhenUsingDefaultTemplate() {
        var payload = JsonParser.parseString("{\"hex\":\"00\"}");
        TcpHexFieldDefinition a = new TcpHexFieldDefinition();
        a.setKey("h");
        a.setByteOffset(0);
        a.setValueType(TcpHexValueType.UINT8);
        a.setFixedWireIntegralValue(165L);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, null, List.of(a), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().has("h")).isFalse();
    }

    /** 命令字已匹配但固定线值与帧不符：该规则作废，继续默认字段 */
    @Test
    void fixedIntegralMismatchOnCommandRuleFallsThroughToDefaultFields() {
        String hex = "a55a10a0a4ff";
        var payload = JsonParser.parseString("{\"hex\":\"" + hex + "\"}");
        TcpHexCommandProfile cmd = new TcpHexCommandProfile();
        cmd.setName("needs_magic_a5");
        cmd.setMatchByteOffset(4);
        cmd.setMatchValueType(TcpHexValueType.UINT8);
        cmd.setMatchValue(0xA4);
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey("h");
        f.setByteOffset(0);
        f.setValueType(TcpHexValueType.UINT8);
        f.setFixedWireIntegralValue(0L);
        cmd.setFields(List.of(f));
        TcpHexFieldDefinition def = new TcpHexFieldDefinition();
        def.setKey("b0");
        def.setByteOffset(0);
        def.setValueType(TcpHexValueType.UINT8);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, List.of(cmd), List.of(def), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().has("hexCmdProfile")).isFalse();
        assertThat(out.get().get("b0").getAsInt()).isEqualTo(0xA5);
    }

    /** 第一条规则固定值失败，第二条仍可按命令字匹配 */
    @Test
    void fixedMismatchSkipsFirstCommandRuleSecondStillMatches() {
        String hex = "a55a10a0a4ff";
        var payload = JsonParser.parseString("{\"hex\":\"" + hex + "\"}");
        TcpHexCommandProfile bad = new TcpHexCommandProfile();
        bad.setName("bad_fixed");
        bad.setMatchByteOffset(4);
        bad.setMatchValueType(TcpHexValueType.UINT8);
        bad.setMatchValue(0xA4);
        TcpHexFieldDefinition badF = new TcpHexFieldDefinition();
        badF.setKey("x");
        badF.setByteOffset(0);
        badF.setValueType(TcpHexValueType.UINT8);
        badF.setFixedWireIntegralValue(0L);
        bad.setFields(List.of(badF));

        TcpHexCommandProfile good = new TcpHexCommandProfile();
        good.setName("ok");
        good.setMatchByteOffset(4);
        good.setMatchValueType(TcpHexValueType.UINT8);
        good.setMatchValue(0xA4);
        TcpHexFieldDefinition goodF = new TcpHexFieldDefinition();
        goodF.setKey("afterCmd");
        goodF.setByteOffset(5);
        goodF.setValueType(TcpHexValueType.UINT8);
        good.setFields(List.of(goodF));

        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, List.of(bad, good), List.of(), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("hexCmdProfile").getAsString()).isEqualTo("ok");
        assertThat(out.get().get("afterCmd").getAsInt()).isEqualTo(0xFF);
    }

    @Test
    void fixedBytesHexMatchEmitsTelemetry() {
        var payload = JsonParser.parseString("{\"hex\":\"a55a\"}");
        TcpHexFieldDefinition a = new TcpHexFieldDefinition();
        a.setKey("magic");
        a.setByteOffset(0);
        a.setValueType(TcpHexValueType.BYTES_AS_HEX);
        a.setByteLength(2);
        a.setFixedBytesHex("A55A");
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, null, List.of(a), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("magic").getAsString()).isEqualTo("a55a");
    }

    @Test
    void commandProfileMatchesBeforeDefaultFields() {
        String hex = "a55a10a0a4ff";
        var payload = JsonParser.parseString("{\"hex\":\"" + hex + "\"}");
        TcpHexCommandProfile cmd = new TcpHexCommandProfile();
        cmd.setName("heartbeat");
        cmd.setMatchByteOffset(4);
        cmd.setMatchValueType(TcpHexValueType.UINT8);
        cmd.setMatchValue(0xA4);
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey("afterCmd");
        f.setByteOffset(5);
        f.setValueType(TcpHexValueType.UINT8);
        cmd.setFields(List.of(f));
        TcpHexFieldDefinition def = new TcpHexFieldDefinition();
        def.setKey("b0");
        def.setByteOffset(0);
        def.setValueType(TcpHexValueType.UINT8);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, List.of(cmd), List.of(def), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("afterCmd").getAsInt()).isEqualTo(0xFF);
        assertThat(out.get().get("hexCmdProfile").getAsString()).isEqualTo("heartbeat");
        assertThat(out.get().has("b0")).isFalse();
    }

    @Test
    void fallbackToDefaultWhenCommandMismatch() {
        String hex = "a55a10a0a4ff";
        var payload = JsonParser.parseString("{\"hex\":\"" + hex + "\"}");
        TcpHexCommandProfile cmd = new TcpHexCommandProfile();
        cmd.setName("other");
        cmd.setMatchByteOffset(4);
        cmd.setMatchValueType(TcpHexValueType.UINT8);
        cmd.setMatchValue(0x21);
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey("never");
        f.setByteOffset(5);
        f.setValueType(TcpHexValueType.UINT8);
        cmd.setFields(List.of(f));
        TcpHexFieldDefinition def = new TcpHexFieldDefinition();
        def.setKey("b0");
        def.setByteOffset(0);
        def.setValueType(TcpHexValueType.UINT8);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, List.of(cmd), List.of(def), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("b0").getAsInt()).isEqualTo(0xA5);
        assertThat(out.get().has("hexCmdProfile")).isFalse();
    }

    /** 主命令相同（如 0xA2）时，用第二匹配字节区分规则 */
    @Test
    void commandProfileSecondaryMatchDistinguishesSamePrimary() {
        String hex = "a55a0101a200022100";
        var payload = JsonParser.parseString("{\"hex\":\"" + hex + "\"}");
        TcpHexCommandProfile cmd21 = new TcpHexCommandProfile();
        cmd21.setName("ack_echo_21");
        cmd21.setMatchByteOffset(4);
        cmd21.setMatchValueType(TcpHexValueType.UINT8);
        cmd21.setMatchValue(0xA2);
        cmd21.setSecondaryMatchByteOffset(7);
        cmd21.setSecondaryMatchValueType(TcpHexValueType.UINT8);
        cmd21.setSecondaryMatchValue(0x21L);
        TcpHexFieldDefinition f21 = new TcpHexFieldDefinition();
        f21.setKey("paramEcho");
        f21.setByteOffset(7);
        f21.setValueType(TcpHexValueType.UINT8);
        cmd21.setFields(List.of(f21));

        TcpHexCommandProfile cmd06 = new TcpHexCommandProfile();
        cmd06.setName("ack_echo_06");
        cmd06.setMatchByteOffset(4);
        cmd06.setMatchValueType(TcpHexValueType.UINT8);
        cmd06.setMatchValue(0xA2);
        cmd06.setSecondaryMatchByteOffset(7);
        cmd06.setSecondaryMatchValueType(TcpHexValueType.UINT8);
        cmd06.setSecondaryMatchValue(0x06L);
        TcpHexFieldDefinition f06 = new TcpHexFieldDefinition();
        f06.setKey("wrong");
        f06.setByteOffset(7);
        f06.setValueType(TcpHexValueType.UINT8);
        cmd06.setFields(List.of(f06));

        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload,
                List.of(cmd21, cmd06), List.of(), null, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("hexCmdProfile").getAsString()).isEqualTo("ack_echo_21");
        assertThat(out.get().get("paramEcho").getAsInt()).isEqualTo(0x21);
    }

    /** LTV: len=2, tag=1, value=11 22 → UINT_AUTO_BE（2 字节）→ UINT16_BE = 0x1122 */
    @Test
    void ltvRepeatingParsesChunks() {
        String hex = "02011122";
        var payload = JsonParser.parseString("{\"hex\":\"" + hex + "\"}");
        TcpHexLtvRepeatingConfig ltv = new TcpHexLtvRepeatingConfig();
        ltv.setStartByteOffset(0);
        ltv.setLengthFieldType(TcpHexValueType.UINT8);
        ltv.setTagFieldType(TcpHexValueType.UINT8);
        ltv.setChunkOrder(TcpHexLtvChunkOrder.LTV);
        ltv.setKeyPrefix("p");
        TcpHexLtvTagMapping m = new TcpHexLtvTagMapping();
        m.setTagValue(1);
        m.setTelemetryKey("val");
        m.setValueType(TcpHexValueType.UINT_AUTO_BE);
        ltv.setTagMappings(List.of(m));
        ltv.setUnknownTagMode(TcpHexUnknownTagMode.SKIP);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, null, null, ltv, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("p_0_val").getAsInt()).isEqualTo(0x1122);
    }

    /**
     * 灵信式 LTV：Length = Tag+Value 总字节数（如 len=3 表示 1 字节 tag + 2 字节 value）。
     */
    @Test
    void ltvRepeatingLengthIncludesTagParsesValueOnly() {
        // UINT8 len=3, UINT8 tag=0x0A, value 11 22 → UINT16_BE = 0x1122
        String hex = "030a1122";
        var payload = JsonParser.parseString("{\"hex\":\"" + hex + "\"}");
        TcpHexLtvRepeatingConfig ltv = new TcpHexLtvRepeatingConfig();
        ltv.setStartByteOffset(0);
        ltv.setLengthFieldType(TcpHexValueType.UINT8);
        ltv.setTagFieldType(TcpHexValueType.UINT8);
        ltv.setChunkOrder(TcpHexLtvChunkOrder.LTV);
        ltv.setLengthIncludesTag(true);
        ltv.setKeyPrefix("lx");
        TcpHexLtvTagMapping m = new TcpHexLtvTagMapping();
        m.setTagValue(10);
        m.setTelemetryKey("val");
        m.setValueType(TcpHexValueType.UINT_AUTO_BE);
        ltv.setTagMappings(List.of(m));
        ltv.setUnknownTagMode(TcpHexUnknownTagMode.SKIP);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, null, null, ltv, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("lx_0_val").getAsInt()).isEqualTo(0x1122);
    }

    /** LTV Tag 映射 BYTES_AS_HEX：长度完全由 Length 切出的 Value 决定，不另填 byteLength */
    @Test
    void ltvTagMappingBytesAsHexUsesWireValueLength() {
        String hex = "0301aabbcc";
        var payload = JsonParser.parseString("{\"hex\":\"" + hex + "\"}");
        TcpHexLtvRepeatingConfig ltv = new TcpHexLtvRepeatingConfig();
        ltv.setStartByteOffset(0);
        ltv.setLengthFieldType(TcpHexValueType.UINT8);
        ltv.setTagFieldType(TcpHexValueType.UINT8);
        ltv.setChunkOrder(TcpHexLtvChunkOrder.LTV);
        ltv.setKeyPrefix("p");
        TcpHexLtvTagMapping m = new TcpHexLtvTagMapping();
        m.setTagValue(1);
        m.setTelemetryKey("raw");
        m.setValueType(TcpHexValueType.BYTES_AS_HEX);
        ltv.setTagMappings(List.of(m));
        ltv.setUnknownTagMode(TcpHexUnknownTagMode.SKIP);
        var out = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(payload, null, null, ltv, null, UUID.randomUUID());
        assertThat(out).isPresent();
        assertThat(out.get().get("p_0_raw").getAsString()).isEqualTo("aabbcc");
    }
}
