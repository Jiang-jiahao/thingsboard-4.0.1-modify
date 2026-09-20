package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.TbContext;
import com.jnks.iot.rule.engine.api.TbNodeConfiguration;
import com.jnks.iot.rule.engine.api.TbNodeException;
import com.jnks.iot.rule.engine.api.util.TbNodeUtils;
import com.jnks.iot.rule.engine.util.TbMsgSource;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.security.DeviceCredentialsType;
import com.jnks.iot.server.common.data.util.TbPair;
import com.jnks.iot.server.common.msg.TbMsg;

import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "fetch device credentials",
        version = 1,
        configClazz = TbFetchDeviceCredentialsNodeConfiguration.class,
        nodeDescription = "Adds device credentials to the message or message metadata",
        nodeDetails = "if message originator type is Device and device credentials was successfully fetched, " +
                "rule node enriches message or message metadata with <i>credentialsType</i> and <i>credentials</i> properties. " +
                "Useful when you need to fetch device credentials and use them for further message processing. " +
                "For example, use device credentials to interact with external systems.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "tbEnrichmentNodeFetchDeviceCredentialsConfig")
public class TbFetchDeviceCredentialsNode extends TbAbstractNodeWithFetchTo<TbFetchDeviceCredentialsNodeConfiguration> {

    private static final String CREDENTIALS = "credentials";
    private static final String CREDENTIALS_TYPE = "credentialsType";

    @Override
    protected TbFetchDeviceCredentialsNodeConfiguration loadNodeConfiguration(TbNodeConfiguration configuration) throws TbNodeException {
        return TbNodeUtils.convert(configuration, TbFetchDeviceCredentialsNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        var originator = msg.getOriginator();
        var msgDataAsObjectNode = TbMsgSource.DATA.equals(fetchTo) ? getMsgDataAsObjectNode(msg) : null;
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
        if (TbMsgSource.METADATA.equals(fetchTo)) {
            metaData.putValue(CREDENTIALS_TYPE, credentialsType.name());
            if (credentialsType.equals(DeviceCredentialsType.ACCESS_TOKEN) || credentialsType.equals(DeviceCredentialsType.X509_CERTIFICATE)) {
                metaData.putValue(CREDENTIALS, credentialsInfo.asText());
            } else {
                metaData.putValue(CREDENTIALS, JacksonUtil.toString(credentialsInfo));
            }
        } else if (TbMsgSource.DATA.equals(fetchTo)) {
            msgDataAsObjectNode.put(CREDENTIALS_TYPE, credentialsType.name());
            msgDataAsObjectNode.set(CREDENTIALS, credentialsInfo);
        }
        TbMsg transformedMsg = transformMessage(msg, msgDataAsObjectNode, metaData);
        ctx.tellSuccess(transformedMsg);
    }

    @Override
    public TbPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws TbNodeException {
        return fromVersion == 0 ?
                upgradeRuleNodesWithOldPropertyToUseFetchTo(
                        oldConfiguration,
                        "fetchToMetadata",
                        TbMsgSource.METADATA.name(),
                        TbMsgSource.DATA.name()) :
                new TbPair<>(false, oldConfiguration);
    }

}
