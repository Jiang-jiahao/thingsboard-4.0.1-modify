package com.jnks.iot.rule.engine.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.util.JnksIotPair;

public abstract class JnksIotAbstractTransformNodeWithJnksIotMsgSource implements JnksIotNode {

    protected static final String FROM_METADATA_PROPERTY = "fromMetadata";

    protected abstract String getNewKeyForUpgradeFromVersionZero();

    protected abstract String getKeyToUpgradeFromVersionOne();

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        ObjectNode configToUpdate = (ObjectNode) oldConfiguration;
        switch (fromVersion) {
            case 0:
                return upgradeToUseJnksIotMsgSource(configToUpdate);
            case 1:
                return upgradeNodesWithVersionOneToUseJnksIotMsgSource(configToUpdate);
            default:
                return new JnksIotPair<>(false, oldConfiguration);
        }
    }

    private JnksIotPair<Boolean, JsonNode> upgradeToUseJnksIotMsgSource(ObjectNode configToUpdate) throws JnksIotNodeException {
        if (!configToUpdate.has(FROM_METADATA_PROPERTY)) {
            throw new JnksIotNodeException("property to update: '" + FROM_METADATA_PROPERTY + "' doesn't exists in configuration!");
        }
        var value = configToUpdate.get(FROM_METADATA_PROPERTY).asText();
        if ("true".equals(value)) {
            configToUpdate.remove(FROM_METADATA_PROPERTY);
            configToUpdate.put(getNewKeyForUpgradeFromVersionZero(), JnksIotMsgSource.METADATA.name());
            return new JnksIotPair<>(true, configToUpdate);
        }
        if ("false".equals(value)) {
            configToUpdate.remove(FROM_METADATA_PROPERTY);
            configToUpdate.put(getNewKeyForUpgradeFromVersionZero(), JnksIotMsgSource.DATA.name());
            return new JnksIotPair<>(true, configToUpdate);
        }
        throw new JnksIotNodeException("property to update: '" + FROM_METADATA_PROPERTY + "' has unexpected value: "
                + value + ". Allowed values: true or false!");
    }

    private JnksIotPair<Boolean, JsonNode> upgradeNodesWithVersionOneToUseJnksIotMsgSource(ObjectNode configToUpdate) throws JnksIotNodeException {
        if (configToUpdate.has(getNewKeyForUpgradeFromVersionZero())) {
            return new JnksIotPair<>(false, configToUpdate);
        }
        return upgradeJnksIotMsgSourceKey(configToUpdate, getKeyToUpgradeFromVersionOne());
    }

    private JnksIotPair<Boolean, JsonNode> upgradeJnksIotMsgSourceKey(ObjectNode configToUpdate, String oldPropertyKey) throws JnksIotNodeException {
        if (!configToUpdate.has(oldPropertyKey)) {
            throw new JnksIotNodeException("property to update: '" + oldPropertyKey + "' doesn't exists in configuration!");
        }
        var value = configToUpdate.get(oldPropertyKey).asText();
        if (JnksIotMsgSource.METADATA.name().equals(value)) {
            configToUpdate.remove(oldPropertyKey);
            configToUpdate.put(getNewKeyForUpgradeFromVersionZero(), JnksIotMsgSource.METADATA.name());
            return new JnksIotPair<>(true, configToUpdate);
        }
        if (JnksIotMsgSource.DATA.name().equals(value)) {
            configToUpdate.remove(oldPropertyKey);
            configToUpdate.put(getNewKeyForUpgradeFromVersionZero(), JnksIotMsgSource.DATA.name());
            return new JnksIotPair<>(true, configToUpdate);
        }
        throw new JnksIotNodeException("property to update: '" + oldPropertyKey + "' has unexpected value: "
                + value + ". Allowed values: true or false!");
    }

}
