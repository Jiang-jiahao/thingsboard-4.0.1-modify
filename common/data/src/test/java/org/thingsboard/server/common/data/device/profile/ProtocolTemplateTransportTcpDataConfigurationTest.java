/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolTemplateTransportTcpDataConfigurationTest {

    @Test
    void expandProducesHexCommandProfilesForUplinkOnly() {
        ProtocolTemplateDefinition tpl = new ProtocolTemplateDefinition();
        tpl.setId("t1");
        tpl.setCommandByteOffset(12);
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey("len");
        f.setByteOffset(0);
        f.setValueType(TcpHexValueType.UINT32_LE);
        tpl.setHexProtocolFields(List.of(f));
        tpl.setValidateTotalLengthU32Le(true);

        ProtocolTemplateCommandDefinition up = new ProtocolTemplateCommandDefinition();
        up.setTemplateId("t1");
        up.setName("status");
        up.setCommandValue(3);
        up.setMatchValueType(TcpHexValueType.UINT32_LE);
        up.setDirection(ProtocolTemplateCommandDirection.UPLINK);

        ProtocolTemplateCommandDefinition down = new ProtocolTemplateCommandDefinition();
        down.setTemplateId("t1");
        down.setCommandValue(2);
        down.setMatchValueType(TcpHexValueType.UINT32_LE);
        down.setDirection(ProtocolTemplateCommandDirection.DOWNLINK);

        ProtocolTemplateTransportTcpDataConfiguration cfg = new ProtocolTemplateTransportTcpDataConfiguration();
        cfg.setProtocolTemplates(List.of(tpl));
        cfg.setProtocolCommands(List.of(up, down));

        HexTransportTcpDataConfiguration hex = cfg.expandToHexTransportTcpDataConfiguration();
        assertThat(hex.getHexCommandProfiles()).hasSize(1);
        assertThat(hex.getHexCommandProfiles().get(0).getMatchValue()).isEqualTo(3);
        assertThat(hex.getValidateTotalLengthU32Le()).isTrue();
    }

    @Test
    void mergeTemplateAndCommandFieldsKeepsNonOverlappingTemplateAndDropsOverlapped() {
        TcpHexFieldDefinition src = field("srcAddr", 2, TcpHexValueType.UINT8);
        TcpHexFieldDefinition paramLo = field("paramFirst", 7, TcpHexValueType.UINT8);
        TcpHexFieldDefinition pa1 = field("pa1_word", 7, TcpHexValueType.UINT16_BE);

        List<TcpHexFieldDefinition> merged = ProtocolTemplateTransportTcpDataConfiguration.mergeTemplateAndCommandFields(
                List.of(src, paramLo), List.of(pa1));
        assertThat(merged).hasSize(2);
        assertThat(merged.get(0).getKey()).isEqualTo("srcAddr");
        assertThat(merged.get(1).getKey()).isEqualTo("pa1_word");
    }

    @Test
    void partialCommandFieldsMergeIntoExpandedProfile() {
        ProtocolTemplateDefinition tpl = new ProtocolTemplateDefinition();
        tpl.setId("t1");
        tpl.setCommandByteOffset(4);
        tpl.setHexProtocolFields(List.of(
                field("srcAddr", 2, TcpHexValueType.UINT8),
                field("paramFirst", 7, TcpHexValueType.UINT8)
        ));

        ProtocolTemplateCommandDefinition up = new ProtocolTemplateCommandDefinition();
        up.setTemplateId("t1");
        up.setName("jamAck");
        up.setCommandValue(0xA2);
        up.setMatchValueType(TcpHexValueType.UINT8);
        up.setDirection(ProtocolTemplateCommandDirection.UPLINK);
        up.setSecondaryMatchByteOffset(7);
        up.setSecondaryMatchValueType(TcpHexValueType.UINT8);
        up.setSecondaryMatchValue(0x21L);
        up.setFields(List.of(
                field("originalCmdEcho", 7, TcpHexValueType.UINT8),
                field("pa1_statusWord", 8, TcpHexValueType.UINT16_BE)
        ));

        ProtocolTemplateTransportTcpDataConfiguration cfg = new ProtocolTemplateTransportTcpDataConfiguration();
        cfg.setProtocolTemplates(List.of(tpl));
        cfg.setProtocolCommands(List.of(up));

        TcpHexCommandProfile p = cfg.expandToHexTransportTcpDataConfiguration().getHexCommandProfiles().get(0);
        assertThat(p.getFields()).hasSize(3);
        assertThat(p.getFields().stream().map(TcpHexFieldDefinition::getKey)).containsExactly(
                "srcAddr", "originalCmdEcho", "pa1_statusWord");
    }

    private static TcpHexFieldDefinition field(String key, int off, TcpHexValueType vt) {
        TcpHexFieldDefinition f = new TcpHexFieldDefinition();
        f.setKey(key);
        f.setByteOffset(off);
        f.setValueType(vt);
        return f;
    }
}
