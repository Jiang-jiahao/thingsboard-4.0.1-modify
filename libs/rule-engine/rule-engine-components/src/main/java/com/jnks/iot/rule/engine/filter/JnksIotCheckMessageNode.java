package com.jnks.iot.rule.engine.filter;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.List;
import java.util.Map;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "检查字段是否存在",
        relationTypes = {JnksIotNodeConnectionType.TRUE, JnksIotNodeConnectionType.FALSE},
        configClazz = JnksIotCheckMessageNodeConfiguration.class,
        nodeDescription = "检查消息和/或元数据中是否存在指定字段。",
        nodeDetails = "默认情况下，规则节点会检查所有指定字段是否都存在。 " +
                "如果只需至少一个字段存在即可，请取消勾选「检查所有选定字段是否都存在」。<br><br>" +
                "输出连接：<code>True</code>、<code>False</code>、<code>Failure</code>",
        configDirective = "jnksIotFilterNodeCheckMessageConfig")
public class JnksIotCheckMessageNode implements JnksIotNode {

    private static final Gson gson = new Gson();

    private JnksIotCheckMessageNodeConfiguration config;
    private List<String> messageNamesList;
    private List<String> metadataNamesList;

    @Override
    public void init(JnksIotContext jnksIotContext, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotCheckMessageNodeConfiguration.class);
        messageNamesList = config.getMessageNames();
        metadataNamesList = config.getMetadataNames();
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        try {
            String relationType = config.isCheckAllKeys() ?
                    allKeysData(msg) && allKeysMetadata(msg) ? JnksIotNodeConnectionType.TRUE : JnksIotNodeConnectionType.FALSE :
                    atLeastOneData(msg) || atLeastOneMetadata(msg) ? JnksIotNodeConnectionType.TRUE : JnksIotNodeConnectionType.FALSE;
            ctx.tellNext(msg, relationType);
        } catch (Exception e) {
            ctx.tellFailure(msg, e);
        }
    }

    private boolean allKeysData(JnksIotMsg msg) {
        if (!messageNamesList.isEmpty()) {
            Map<String, String> dataMap = dataToMap(msg);
            return processAllKeys(messageNamesList, dataMap);
        }
        return true;
    }

    private boolean allKeysMetadata(JnksIotMsg msg) {
        if (!metadataNamesList.isEmpty()) {
            Map<String, String> metadataMap = metadataToMap(msg);
            return processAllKeys(metadataNamesList, metadataMap);
        }
        return true;
    }

    private boolean atLeastOneData(JnksIotMsg msg) {
        if (!messageNamesList.isEmpty()) {
            Map<String, String> dataMap = dataToMap(msg);
            return processAtLeastOne(messageNamesList, dataMap);
        }
        return false;
    }

    private boolean atLeastOneMetadata(JnksIotMsg msg) {
        if (!metadataNamesList.isEmpty()) {
            Map<String, String> metadataMap = metadataToMap(msg);
            return processAtLeastOne(metadataNamesList, metadataMap);
        }
        return false;
    }

    private boolean processAllKeys(List<String> data, Map<String, String> map) {
        for (String field : data) {
            if (!map.containsKey(field)) {
                return false;
            }
        }
        return true;
    }

    private boolean processAtLeastOne(List<String> data, Map<String, String> map) {
        for (String field : data) {
            if (map.containsKey(field)) {
                return true;
            }
        }
        return false;
    }

    private Map<String, String> metadataToMap(JnksIotMsg msg) {
        return msg.getMetaData().getData();
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> dataToMap(JnksIotMsg msg) {
        return (Map<String, String>) gson.fromJson(msg.getData(), Map.class);
    }

}