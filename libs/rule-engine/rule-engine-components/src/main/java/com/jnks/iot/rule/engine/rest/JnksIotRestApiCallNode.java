package com.jnks.iot.rule.engine.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.external.JnksIotAbstractExternalNode;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.List;

@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "REST API 调用",
        configClazz = JnksIotRestApiCallNodeConfiguration.class,
        version = 3,
        nodeDescription = "向外部 REST 服务器发起 REST API 调用",
        nodeDetails = "将向外部 REST 服务器发起 REST API 调用 <code>GET | POST | PUT | DELETE</code>。 " +
                "消息负载会加入请求体。配置的属性可以从消息元数据加入请求头。" +
                " 出站消息会在消息元数据中包含响应字段 " +
                "（<code>status</code>、<code>statusCode</code>、<code>statusReason</code> 及响应 <code>headers</code>）。" +
                " 响应体保存在出站消息负载中。 " +
                "例如 <b>statusCode</b> 字段可通过 <code>metadata.statusCode</code> 访问。" +
                "<br/><b>注意：</b>若使用系统代理属性，应添加以下系统代理属性：\"http.proxyHost\" 和 \"http.proxyPort\"，或 \"https.proxyHost\" 和 \"https.proxyPort\"，或 \"socksProxyHost\" 和 \"socksProxyPort\"，" +
                "如果代理需要认证，则还应将 \"tb.proxy.user\" 和 \"tb.proxy.password\" 添加到 jnks-iot.conf 文件中。",
        configDirective = "jnksIotExternalNodeRestApiCallConfig",
        iconUrl = "data:image/svg+xml;base64,PHN2ZyBzdHlsZT0iZW5hYmxlLWJhY2tncm91bmQ6bmV3IDAgMCA1MTIgNTEyIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbDpzcGFjZT0icHJlc2VydmUiIHZpZXdCb3g9IjAgMCA1MTIgNTEyIiB2ZXJzaW9uPSIxLjEiIHk9IjBweCIgeD0iMHB4Ij48ZyB0cmFuc2Zvcm09Im1hdHJpeCguOTQ5NzUgMCAwIC45NDk3NSAxNy4xMiAyNi40OTIpIj48cGF0aCBkPSJtMTY5LjExIDEwOC41NGMtOS45MDY2IDAuMDczNC0xOS4wMTQgNi41NzI0LTIyLjAxNCAxNi40NjlsLTY5Ljk5MyAyMzEuMDhjLTMuNjkwNCAxMi4xODEgMy4yODkyIDI1LjIyIDE1LjQ2OSAyOC45MSAyLjIyNTkgMC42NzQ4MSA0LjQ5NjkgMSA2LjcyODUgMSA5Ljk3MjEgMCAxOS4xNjUtNi41MTUzIDIyLjE4Mi0xNi40NjdhNi41MjI0IDYuNTIyNCAwIDAgMCAwLjAwMiAtMC4wMDJsNjkuOTktMjMxLjA3YTYuNTIyNCA2LjUyMjQgMCAwIDAgMCAtMC4wMDJjMy42ODU1LTEyLjE4MS0zLjI4Ny0yNS4yMjUtMTUuNDcxLTI4LjkxMi0yLjI4MjUtMC42OTE0NS00LjYxMTYtMS4wMTY5LTYuODk4NC0xem04NC45ODggMGMtOS45MDQ4IDAuMDczNC0xOS4wMTggNi41Njc1LTIyLjAxOCAxNi40NjlsLTY5Ljk4NiAyMzEuMDhjLTMuNjg5OCAxMi4xNzkgMy4yODUzIDI1LjIxNyAxNS40NjUgMjguOTA4IDIuMjI5NyAwLjY3NjQ3IDQuNTAwOCAxLjAwMiA2LjczMjQgMS4wMDIgOS45NzIxIDAgMTkuMTY1LTYuNTE1MyAyMi4xODItMTYuNDY3YTYuNTIyNCA2LjUyMjQgMCAwIDAgMC4wMDIgLTAuMDAybDY5Ljk4OC0yMzEuMDdjMy42OTA4LTEyLjE4MS0zLjI4NTItMjUuMjIzLTE1LjQ2Ny0yOC45MTItMi4yODE0LTAuNjkyMzEtNC42MTA4LTEuMDE4OS02Ljg5ODQtMS4wMDJ6bS0yMTcuMjkgNDIuMjNjLTEyLjcyOS0wLjAwMDg3LTIzLjE4OCAxMC40NTYtMjMuMTg4IDIzLjE4NiAwLjAwMSAxMi43MjggMTAuNDU5IDIzLjE4NiAyMy4xODggMjMuMTg2IDEyLjcyNy0wLjAwMSAyMy4xODMtMTAuNDU5IDIzLjE4NC0yMy4xODYgMC4wMDA4NzYtMTIuNzI4LTEwLjQ1Ni0yMy4xODUtMjMuMTg0LTIzLjE4NnptMCAxNDYuNjRjLTEyLjcyNy0wLjAwMDg3LTIzLjE4NiAxMC40NTUtMjMuMTg4IDIzLjE4NC0wLjAwMDg3MyAxMi43MjkgMTAuNDU4IDIzLjE4OCAyMy4xODggMjMuMTg4IDEyLjcyOC0wLjAwMSAyMy4xODQtMTAuNDYgMjMuMTg0LTIzLjE4OC0wLjAwMS0xMi43MjYtMTAuNDU3LTIzLjE4My0yMy4xODQtMjMuMTg0em0yNzAuNzkgNDIuMjExYy0xMi43MjcgMC0yMy4xODQgMTAuNDU3LTIzLjE4NCAyMy4xODRzMTAuNDU1IDIzLjE4OCAyMy4xODQgMjMuMTg4aDE1NC45OGMxMi43MjkgMCAyMy4xODYtMTAuNDYgMjMuMTg2LTIzLjE4OCAwLjAwMS0xMi43MjgtMTAuNDU4LTIzLjE4NC0yMy4xODYtMjMuMTg0eiIgdHJhbnNmb3JtPSJtYXRyaXgoMS4wMzc2IDAgMCAxLjAzNzYgLTcuNTY3NiAtMTQuOTI1KSIgc3Ryb2tlLXdpZHRoPSIxLjI2OTMiLz48L2c+PC9zdmc+"
)
public class JnksIotRestApiCallNode extends JnksIotAbstractExternalNode {

    static final String PARSE_TO_PLAIN_TEXT = "parseToPlainText";
    static final String MAX_IN_MEMORY_BUFFER_SIZE_IN_KB = "maxInMemoryBufferSizeInKb";
    static final String TRIM_DOUBLE_QUOTES = "trimDoubleQuotes";
    protected JnksIotHttpClient httpClient;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx);
        JnksIotRestApiCallNodeConfiguration config = JnksIotNodeUtils.convert(configuration, JnksIotRestApiCallNodeConfiguration.class);
        httpClient = new JnksIotHttpClient(config, ctx.getSharedEventLoop());
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        var jnksIotMsg = ackIfNeeded(ctx, msg);
        httpClient.processMessage(ctx, jnksIotMsg,
                m -> tellSuccess(ctx, m),
                (m, t) -> tellFailure(ctx, m, t));
    }

    @Override
    public void destroy() {
        if (this.httpClient != null) {
            this.httpClient.destroy();
        }
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0:
                if (!oldConfiguration.has(PARSE_TO_PLAIN_TEXT) && oldConfiguration.has(TRIM_DOUBLE_QUOTES)) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).put(PARSE_TO_PLAIN_TEXT, oldConfiguration.get(TRIM_DOUBLE_QUOTES).booleanValue());
                    ((ObjectNode) oldConfiguration).remove(TRIM_DOUBLE_QUOTES);
                }
            case 1:
                if (oldConfiguration.has("useRedisQueueForMsgPersistence")) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).remove(List.of("useRedisQueueForMsgPersistence", "trimQueue", "maxQueueSize"));
                }
            case 2:
                if (!oldConfiguration.has(MAX_IN_MEMORY_BUFFER_SIZE_IN_KB)) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).put(MAX_IN_MEMORY_BUFFER_SIZE_IN_KB, 256);
                }
                break;
            default:
                break;
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

}
