package com.jnks.iot.rule.engine.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "复制键值对",
        version = 2,
        configClazz = JnksIotCopyKeysNodeConfiguration.class,
        nodeDescription = "在消息与消息元数据之间复制键值对。",
        nodeDetails = "根据配置的方向和键，在消息与消息元数据之间双向复制键值对。 " +
                "可使用正则表达式指定要复制的键值对。源中不存在的已配置键将被忽略。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotTransformationNodeCopyKeysConfig",
        icon = "content_copy"
)
public class JnksIotCopyKeysNode extends JnksIotAbstractTransformNodeWithJnksIotMsgSource {

    private JnksIotCopyKeysNodeConfiguration config;
    private JnksIotMsgSource copyFrom;
    private List<Pattern> compiledKeyPatterns;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotCopyKeysNodeConfiguration.class);
        this.copyFrom = config.getCopyFrom();
        if (copyFrom == null) {
            throw new JnksIotNodeException("CopyFrom can't be null! Allowed values: " + Arrays.toString(JnksIotMsgSource.values()));
        }
        this.compiledKeyPatterns = config.getKeys().stream().map(Pattern::compile).collect(Collectors.toList());
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        var metaDataCopy = msg.getMetaData().copy();
        String msgData = msg.getData();
        boolean msgChanged = false;
        JsonNode dataNode = JacksonUtil.toJsonNode(msgData);
        if (dataNode.isObject()) {
            switch (copyFrom) {
                case METADATA:
                    ObjectNode msgDataNode = (ObjectNode) dataNode;
                    Map<String, String> metaDataMap = metaDataCopy.getData();
                    for (Map.Entry<String, String> entry : metaDataMap.entrySet()) {
                        String mdKey = entry.getKey();
                        String mdValue = entry.getValue();
                        if (matches(mdKey)) {
                            msgChanged = true;
                            msgDataNode.put(mdKey, mdValue);
                        }
                    }
                    msgData = JacksonUtil.toString(msgDataNode);
                    break;
                case DATA:
                    Iterator<Map.Entry<String, JsonNode>> iteratorNode = dataNode.fields();
                    while (iteratorNode.hasNext()) {
                        Map.Entry<String, JsonNode> entry = iteratorNode.next();
                        String msgKey = entry.getKey();
                        JsonNode msgValue = entry.getValue();
                        if (matches(msgKey)) {
                            msgChanged = true;
                            String value = msgValue.isTextual() ?
                                    msgValue.asText() : JacksonUtil.toString(msgValue);
                            metaDataCopy.putValue(msgKey, value);
                        }
                    }
                    break;
                default:
                    log.debug("Unexpected CopyFrom value: {}. Allowed values: {}", copyFrom, JnksIotMsgSource.values());
            }
        }
        ctx.tellSuccess(msgChanged ? msg.transform()
                .metaData(metaDataCopy)
                .data(msgData)
                .build() : msg);
    }

    @Override
    protected String getNewKeyForUpgradeFromVersionZero() {
        return "copyFrom";
    }

    @Override
    protected String getKeyToUpgradeFromVersionOne() {
        return FROM_METADATA_PROPERTY;
    }

    boolean matches(String key) {
        return compiledKeyPatterns.stream().anyMatch(pattern -> pattern.matcher(key).matches());
    }

}
