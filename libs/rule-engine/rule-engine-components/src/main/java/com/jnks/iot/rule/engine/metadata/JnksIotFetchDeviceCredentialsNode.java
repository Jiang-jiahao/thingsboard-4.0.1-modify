package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.security.DeviceCredentialsType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "fetch device credentials",
        version = 1,
        configClazz = JnksIotFetchDeviceCredentialsNodeConfiguration.class,
        nodeDescription = "Adds device credentials to the message or message metadata",
        nodeDetails = "if message originator type is Device and device credentials was successfully fetched, " +
                "rule node enriches message or message metadata with <i>credentialsType</i> and <i>credentials</i> properties. " +
                "Useful when you need to fetch device credentials and use them for further message processing. " +
                "For example, use device credentials to interact with external systems.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "jnksIotEnrichmentNodeFetchDeviceCredentialsConfig")
public class JnksIotFetchDeviceCredentialsNode extends JnksIotAbstractNodeWithFetchTo<JnksIotFetchDeviceCredentialsNodeConfiguration> {

    private static final String CREDENTIALS = "credentials";
    private static final String CREDENTIALS_TYPE = "credentialsType";

    @Override
    protected JnksIotFetchDeviceCredentialsNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        return JnksIotNodeUtils.convert(configuration, JnksIotFetchDeviceCredentialsNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        var originator = msg.getOriginator();
        var msgDataAsObjectNode = JnksIotMsgSource.DATA.equals(fetchTo) ? getMsgDataAsObjectNode(msg) : null;
        if (!EntityType.DEVICE.equals(originator.getEntityType())) {
            ctx.tellFailure(msg, new RuntimeException("Unsupported originator type: " + originator.getEntityType() + "!"));
            return;
        }

        var deviceId = new DeviceId(msg.getOriginator().getId());
        var deviceCredentials = ctx.getDeviceCredentialsService().findDeviceCredentialsByDeviceId(ctx.getTenantId(), deviceId);
        if (deviceCredentials == null) {
            ctx.tellFailure(msg, new RuntimeException("Failed to get Device Credentials for device: " + deviceId + "!"));
            return;
        }
        var credentialsType = deviceCredentials.getCredentialsType();
        var credentialsInfo = ctx.getDeviceCredentialsService().toCredentialsInfo(deviceCredentials);
        var metaData = msg.getMetaData().copy();
        if (JnksIotMsgSource.METADATA.equals(fetchTo)) {
            metaData.putValue(CREDENTIALS_TYPE, credentialsType.name());
            if (credentialsType.equals(DeviceCredentialsType.ACCESS_TOKEN) || credentialsType.equals(DeviceCredentialsType.X509_CERTIFICATE)) {
                metaData.putValue(CREDENTIALS, credentialsInfo.asText());
            } else {
                metaData.putValue(CREDENTIALS, JacksonUtil.toString(credentialsInfo));
            }
        } else if (JnksIotMsgSource.DATA.equals(fetchTo)) {
            msgDataAsObjectNode.put(CREDENTIALS_TYPE, credentialsType.name());
            msgDataAsObjectNode.set(CREDENTIALS, credentialsInfo);
        }
        JnksIotMsg transformedMsg = transformMessage(msg, msgDataAsObjectNode, metaData);
        ctx.tellSuccess(transformedMsg);
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ?
                upgradeRuleNodesWithOldPropertyToUseFetchTo(
                        oldConfiguration,
                        "fetchToMetadata",
                        JnksIotMsgSource.METADATA.name(),
                        JnksIotMsgSource.DATA.name()) :
                new JnksIotPair<>(false, oldConfiguration);
    }

}
