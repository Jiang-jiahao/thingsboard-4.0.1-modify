package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.concurrent.ExecutionException;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
        name = "来源方字段",
        configClazz = JnksIotGetOriginatorFieldsConfiguration.class,
        version = 1,
        nodeDescription = "将消息来源方的字段值添加到消息或消息元数据中",
        nodeDetails = "获取映射中指定的字段值。如果指定的字段不属于来源方字段，则会被忽略。 " +
                "当你需要获取来源方字段并将其用于后续消息处理时非常有用。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotEnrichmentNodeOriginatorFieldsConfig")
public class JnksIotGetOriginatorFieldsNode extends JnksIotAbstractGetMappedDataNode<EntityId, JnksIotGetOriginatorFieldsConfiguration> {

    protected final static String DATA_MAPPING_PROPERTY_NAME = "dataMapping";
    protected static final String OLD_DATA_MAPPING_PROPERTY_NAME = "fieldsMapping";

    @Override
    protected JnksIotGetOriginatorFieldsConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var config = JnksIotNodeUtils.convert(configuration, JnksIotGetOriginatorFieldsConfiguration.class);
        checkIfMappingIsNotEmptyOrElseThrow(config.getDataMapping());
        return config;
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        var msgDataAsJsonNode = JnksIotMsgSource.DATA.equals(fetchTo) ? getMsgDataAsObjectNode(msg) : null;
        processFieldsData(ctx, msg, msg.getOriginator(), msgDataAsJsonNode, config.isIgnoreNullStrings());
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        if (fromVersion == 0) {
            var newConfigObjectNode = (ObjectNode) oldConfiguration;
            if (!newConfigObjectNode.has(OLD_DATA_MAPPING_PROPERTY_NAME)) {
                throw new JnksIotNodeException("property to update: '" + OLD_DATA_MAPPING_PROPERTY_NAME + "' doesn't exists in configuration!");
            }
            newConfigObjectNode.set(DATA_MAPPING_PROPERTY_NAME, newConfigObjectNode.get(OLD_DATA_MAPPING_PROPERTY_NAME));
            newConfigObjectNode.remove(OLD_DATA_MAPPING_PROPERTY_NAME);
            newConfigObjectNode.put(FETCH_TO_PROPERTY_NAME, JnksIotMsgSource.METADATA.name());
            return new JnksIotPair<>(true, newConfigObjectNode);
        }
        return new JnksIotPair<>(false, oldConfiguration);
    }

}
