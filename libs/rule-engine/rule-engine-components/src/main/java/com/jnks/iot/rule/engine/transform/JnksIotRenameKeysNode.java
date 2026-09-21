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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "rename keys",
        version = 2,
        configClazz = JnksIotRenameKeysNodeConfiguration.class,
        nodeDescription = "Renames message or message metadata keys.",
        nodeDetails = "Renames keys in the message or message metadata according to the provided mapping. " +
                "If key to rename doesn't exist in the specified source (message or message metadata) it will be ignored.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "jnksIotTransformationNodeRenameKeysConfig",
        icon = "find_replace"
)
public class JnksIotRenameKeysNode extends JnksIotAbstractTransformNodeWithJnksIotMsgSource {

    private JnksIotRenameKeysNodeConfiguration config;
    private Map<String, String> renameKeysMapping;
    private JnksIotMsgSource renameIn;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotRenameKeysNodeConfiguration.class);
        this.renameIn = config.getRenameIn();
        this.renameKeysMapping = config.getRenameKeysMapping();
        if (renameIn == null) {
            throw new JnksIotNodeException("RenameIn can't be null! Allowed values: " + Arrays.toString(JnksIotMsgSource.values()));
        }
        if (renameKeysMapping == null || renameKeysMapping.isEmpty()) {
            throw new JnksIotNodeException("At least one mapping entry should be specified!");
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        JnksIotMsgMetaData metaDataCopy = msg.getMetaData().copy();
        String data = msg.getData();
        boolean msgChanged = false;
        switch (renameIn) {
            case METADATA:
                Map<String, String> metaDataMap = metaDataCopy.getData();
                for (Map.Entry<String, String> entry : renameKeysMapping.entrySet()) {
                    String currentKeyName = entry.getKey();
                    String newKeyName = entry.getValue();
                    if (metaDataMap.containsKey(currentKeyName)) {
                        msgChanged = true;
                        String value = metaDataMap.get(currentKeyName);
                        metaDataMap.put(newKeyName, value);
                        metaDataMap.remove(currentKeyName);
                    }
                }
                metaDataCopy = new JnksIotMsgMetaData(metaDataMap);
                break;
            case DATA:
                JsonNode dataNode = JacksonUtil.toJsonNode(data);
                if (dataNode.isObject()) {
                    ObjectNode msgData = (ObjectNode) dataNode;
                    for (Map.Entry<String, String> entry : renameKeysMapping.entrySet()) {
                        String currentKeyName = entry.getKey();
                        String newKeyName = entry.getValue();
                        if (msgData.has(currentKeyName)) {
                            msgChanged = true;
                            JsonNode value = msgData.get(currentKeyName);
                            msgData.set(newKeyName, value);
                            msgData.remove(currentKeyName);
                        }
                    }
                    data = JacksonUtil.toString(msgData);
                }
                break;
            default:
                log.debug("Unexpected RenameIn value: {}. Allowed values: {}", renameIn, JnksIotMsgSource.values());
        }
        ctx.tellSuccess(msgChanged ? msg.transform()
                .metaData(metaDataCopy)
                .data(data)
                .build() : msg);
    }

    @Override
    protected String getNewKeyForUpgradeFromVersionZero() {
        return "renameIn";
    }

    @Override
    protected String getKeyToUpgradeFromVersionOne() {
        return FROM_METADATA_PROPERTY;
    }

}
