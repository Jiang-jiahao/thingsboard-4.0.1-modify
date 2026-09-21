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
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "删除键值对",
        version = 2,
        configClazz = JnksIotDeleteKeysNodeConfiguration.class,
        nodeDescription = "从消息或消息元数据中删除键值对。",
        nodeDetails = "根据配置的 " +
                "键和/或正则表达式，从消息或消息元数据中删除键值对。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotTransformationNodeDeleteKeysConfig",
        icon = "remove_circle"
)
public class JnksIotDeleteKeysNode extends JnksIotAbstractTransformNodeWithJnksIotMsgSource {

    private JnksIotDeleteKeysNodeConfiguration config;
    private JnksIotMsgSource deleteFrom;
    private List<Pattern> compiledKeyPatterns;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotDeleteKeysNodeConfiguration.class);
        this.deleteFrom = config.getDeleteFrom();
        if (deleteFrom == null) {
            throw new JnksIotNodeException("DeleteFrom can't be null! Allowed values: " + Arrays.toString(JnksIotMsgSource.values()));
        }
        this.compiledKeyPatterns = config.getKeys().stream().map(Pattern::compile).collect(Collectors.toList());
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        var metaDataCopy = msg.getMetaData().copy();
        var msgDataStr = msg.getData();
        boolean hasNoChanges = false;
        switch (deleteFrom) {
            case METADATA:
                var metaDataMap = metaDataCopy.getData();
                var mdKeysToDelete = metaDataMap.keySet()
                        .stream()
                        .filter(this::matches)
                        .collect(Collectors.toList());
                mdKeysToDelete.forEach(metaDataMap::remove);
                metaDataCopy = new JnksIotMsgMetaData(metaDataMap);
                hasNoChanges = mdKeysToDelete.isEmpty();
                break;
            case DATA:
                JsonNode dataNode = JacksonUtil.toJsonNode(msgDataStr);
                if (dataNode.isObject()) {
                    var msgDataObject = (ObjectNode) dataNode;
                    var msgKeysToDelete = new ArrayList<String>();
                    dataNode.fieldNames().forEachRemaining(key -> {
                        if (matches(key)) {
                            msgKeysToDelete.add(key);
                        }
                    });
                    msgDataObject.remove(msgKeysToDelete);
                    msgDataStr = JacksonUtil.toString(msgDataObject);
                    hasNoChanges = msgKeysToDelete.isEmpty();
                }
                break;
            default:
                log.debug("Unexpected DeleteFrom value: {}. Allowed values: {}", deleteFrom, JnksIotMsgSource.values());
        }
        ctx.tellSuccess(hasNoChanges ? msg : msg.transform()
                .metaData(metaDataCopy)
                .data(msgDataStr)
                .build());
    }

    @Override
    protected String getNewKeyForUpgradeFromVersionZero() {
        return "deleteFrom";
    }

    @Override
    protected String getKeyToUpgradeFromVersionOne() {
        return "dataToFetch";
    }

    boolean matches(String key) {
        return compiledKeyPatterns.stream().anyMatch(pattern -> pattern.matcher(key).matches());
    }

}
