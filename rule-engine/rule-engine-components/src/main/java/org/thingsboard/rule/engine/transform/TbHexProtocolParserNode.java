/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.rule.engine.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.rule.engine.transform.hexparser.HexProtocolDefinition;
import org.thingsboard.rule.engine.transform.hexparser.HexProtocolExpander;
import org.thingsboard.rule.engine.transform.hexparser.HexProtocolParser;
import org.thingsboard.rule.engine.transform.hexparser.TbHexProtocolParserNodeConfiguration;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Parses a hex string field using declarative per-protocol definitions (fixed fields, TLV list, checksums).
 */
@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "Hex protocol parser",
        configClazz = TbHexProtocolParserNodeConfiguration.class,
        nodeDescription = "Parses binary payloads from a continuous hex string using configurable protocol definitions.",
        nodeDetails = "Reads <code>hexInputKey</code> from incoming JSON message body. " +
                "Optional <code>frameTemplates</code>: sync (may be empty), <code>headerFields</code>, optional default checksum; protocols set <code>templateId</code>, payload fields, checksum per command. " +
                "Everything is driven by the node configuration UI—no code changes. " +
                "Selects protocol by <code>syncHex</code> (or template sync) and optional <code>commandByteOffset</code> + <code>commandValue</code> (or <code>commandMatchWidth</code>=4 for uint32 LE), " +
                "or when there is no sync, by command + <code>minBytes</code>. " +
                "If <code>protocolIdKey</code> is set and the message contains a matching protocol <code>id</code>, that entry is used only when the buffer also matches that definition; otherwise auto-detection runs. " +
                "Writes parsed fields under <code>resultObjectKey</code> (default <code>parsed</code>) or merges with prefix. " +
                "Field types: UINT8, UINT16_LE/BE, UINT32_LE/BE, FLOAT32_LE/BE, FLOAT64_LE/BE, HEX_SLICE, HEX_SLICE_LEN_U16LE, BOOL_BIT, TLV_LIST (optional tlvNestedRules per paramId). " +
                "Checksum: SUM8, CRC16_MODBUS (2 bytes LE at index), CRC16_CCITT (2 bytes BE at index), CRC32 (4 bytes LE at index).<br/><br/>" +
                "Output: <code>Success</code> / <code>Failure</code>.",
        configDirective = "tbTransformationNodeHexProtocolParserConfig",
        icon = "developer_board"
)
public class TbHexProtocolParserNode extends TbAbstractTransformNode<TbHexProtocolParserNodeConfiguration> {

    private TbHexProtocolParserNodeConfiguration config;

    @Override
    protected TbHexProtocolParserNodeConfiguration loadNodeConfiguration(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbHexProtocolParserNodeConfiguration.class);
        if (this.config.getProtocols() == null || this.config.getProtocols().isEmpty()) {
            throw new TbNodeException("At least one protocol definition is required");
        }
        if (this.config.getHexInputKey() == null || this.config.getHexInputKey().isEmpty()) {
            this.config.setHexInputKey("rawHex");
        }
        if (this.config.getResultObjectKey() == null) {
            this.config.setResultObjectKey("parsed");
        }
        if (this.config.getFrameTemplates() == null) {
            this.config.setFrameTemplates(Collections.emptyList());
        }
        return this.config;
    }

    @Override
    protected ListenableFuture<List<TbMsg>> transform(TbContext ctx, TbMsg msg) {
        try {
            JsonNode root = JacksonUtil.toJsonNode(msg.getData());
            if (!root.isObject()) {
                return Futures.immediateFailedFuture(new IllegalArgumentException("Message body must be a JSON object"));
            }
            ObjectNode obj = (ObjectNode) root;
            String hexKey = config.getHexInputKey();
            JsonNode hexNode = obj.get(hexKey);
            if (hexNode == null || !hexNode.isTextual()) {
                return Futures.immediateFailedFuture(new IllegalArgumentException("Missing text field: " + hexKey));
            }
            String protocolId = "";
            if (config.getProtocolIdKey() != null && !config.getProtocolIdKey().isEmpty()) {
                JsonNode p = obj.get(config.getProtocolIdKey());
                if (p != null) {
                    if (p.isTextual()) {
                        protocolId = p.asText();
                    } else if (p.isIntegralNumber()) {
                        protocolId = Long.toString(p.longValue());
                    }
                }
            }
            byte[] buf = HexProtocolParser.parseHexString(hexNode.asText());
            HexProtocolDefinition def = HexProtocolParser.findProtocol(config.getProtocols(), protocolId, buf,
                    Optional.ofNullable(config.getFrameTemplates()).orElse(Collections.emptyList()));
            def = HexProtocolExpander.expand(def,
                    Optional.ofNullable(config.getFrameTemplates()).orElse(Collections.emptyList()));
            ObjectNode parsed = HexProtocolParser.parse(def, buf);

            String prefix = config.getOutputKeyPrefix() != null ? config.getOutputKeyPrefix() : "";
            String resultKey = config.getResultObjectKey();
            if (resultKey != null && !resultKey.isEmpty()) {
                obj.set(resultKey, parsed);
            } else {
                parsed.fields().forEachRemaining(e -> obj.set(prefix + e.getKey(), e.getValue()));
            }
            TbMsg out = msg.transform().data(JacksonUtil.toString(obj)).build();
            return Futures.immediateFuture(Collections.singletonList(out));
        } catch (Exception e) {
            log.debug("Hex parse failed: {}", e.getMessage());
            return Futures.immediateFailedFuture(e);
        }
    }

}
