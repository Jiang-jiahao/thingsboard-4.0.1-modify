package com.jnks.iot.rule.engine.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.transform.hexparser.HexProtocolDefinition;
import com.jnks.iot.rule.engine.transform.hexparser.HexProtocolExpander;
import com.jnks.iot.rule.engine.transform.hexparser.HexProtocolParser;
import com.jnks.iot.rule.engine.transform.hexparser.JnksIotHexProtocolParserNodeConfiguration;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Parses a hex string field using declarative per-protocol definitions (fixed fields, TLV list, checksums).
 */
@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "Hex 协议解析器",
        configClazz = JnksIotHexProtocolParserNodeConfiguration.class,
        nodeDescription = "使用可配置的协议定义，从连续的十六进制字符串中解析二进制负载。",
        nodeDetails = "从传入的 JSON 消息正文中读取 <code>hexInputKey</code>。 " +
                "帧模板定义同步字、头部字段、负载布局提示以及可选的默认校验和；每个协议变体选择一个 <code>templateId</code>，并定义特定于响应的负载字段（或不使用模板定义完整布局）。 " +
                "匹配：<code>syncHex</code>（或来自模板），可选的 <code>commandByteOffset</code> + <code>commandValue</code>（对于 uint32 LE，<code>commandMatchWidth</code>=4）；省略 <code>commandValue</code> 的 headless 模式可匹配任意命令（共享布局）。 " +
                "如果设置了 <code>protocolIdKey</code>，则仅当缓冲区也匹配时才使用该 <code>id</code>；否则自动检测。 " +
                "输出到 <code>resultObjectKey</code> 下（默认 <code>parsed</code>），或使用前缀合并。 " +
                "标量 / 切片类型：UINT8、UINT16_LE/BE、UINT32_LE/BE、FLOAT32/64 LE/BE、HEX_SLICE、HEX_SLICE_LEN_U16LE、BOOL_BIT。 " +
                "可组合类型：STRUCT（嵌套 <code>nestedFields</code>，偏移量相对于结构体起始位置）、TLV_LIST（UI 标签 LIST）、UNIT_LIST， " +
                "GENERIC_LIST（区域 + 计数模式 FIXED | FROM_FIELD | UNTIL_END，条目长度为 FIXED 或 PREFIX_UINT8/UINT16/UINT32，<code>listItemFields</code> 作为每个元素的子协议）。 " +
                "校验和：SUM8、CRC16_MODBUS、CRC16_CCITT、CRC32、NONE。<br/><br/>" +
                "输出：<code>Success</code> / <code>Failure</code>。",
        configDirective = "jnksIotTransformationNodeHexProtocolParserConfig",
        icon = "developer_board"
)
public class JnksIotHexProtocolParserNode extends JnksIotAbstractTransformNode<JnksIotHexProtocolParserNodeConfiguration> {

    private JnksIotHexProtocolParserNodeConfiguration config;

    @Override
    protected JnksIotHexProtocolParserNodeConfiguration loadNodeConfiguration(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotHexProtocolParserNodeConfiguration.class);
        if (this.config.getProtocols() == null || this.config.getProtocols().isEmpty()) {
            throw new JnksIotNodeException("At least one protocol definition is required");
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
    protected ListenableFuture<List<JnksIotMsg>> transform(JnksIotContext ctx, JnksIotMsg msg) {
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
            JnksIotMsg out = msg.transform().data(JacksonUtil.toString(obj)).build();
            return Futures.immediateFuture(Collections.singletonList(out));
        } catch (Exception e) {
            log.debug("Hex parse failed: {}", e.getMessage());
            return Futures.immediateFailedFuture(e);
        }
    }

}
