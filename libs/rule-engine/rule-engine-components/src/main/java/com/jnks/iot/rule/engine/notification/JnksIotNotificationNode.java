package com.jnks.iot.rule.engine.notification;

import com.google.common.util.concurrent.FutureCallback;
import com.jnks.iot.common.util.DonAsynchron;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.external.JnksIotAbstractExternalNode;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.notification.NotificationRequest;
import com.jnks.iot.server.common.data.notification.NotificationRequestConfig;
import com.jnks.iot.server.common.data.notification.NotificationRequestStats;
import com.jnks.iot.server.common.data.notification.info.RuleEngineOriginatedNotificationInfo;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.concurrent.ExecutionException;

@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "send notification",
        configClazz = JnksIotNotificationNodeConfiguration.class,
        nodeDescription = "Sends notification to targets using the template",
        nodeDetails = "Will send notification to the specified targets using the template",
        configDirective = "jnksIotExternalNodeNotificationConfig",
        icon = "notifications"
)
public class JnksIotNotificationNode extends JnksIotAbstractExternalNode {

    private JnksIotNotificationNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx);
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotNotificationNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        RuleEngineOriginatedNotificationInfo notificationInfo = RuleEngineOriginatedNotificationInfo.builder()
                .msgOriginator(msg.getOriginator())
                .msgCustomerId(msg.getOriginator().getEntityType() == EntityType.CUSTOMER
                        && msg.getOriginator().equals(msg.getCustomerId()) ? null : msg.getCustomerId())
                .msgMetadata(msg.getMetaData().getData())
                .msgData(JacksonUtil.toFlatMap(JacksonUtil.toJsonNode(msg.getData())))
                .msgType(msg.getType())
                .build();

        NotificationRequest notificationRequest = NotificationRequest.builder()
                .tenantId(ctx.getTenantId())
                .targets(config.getTargets())
                .templateId(config.getTemplateId())
                .info(notificationInfo)
                .additionalConfig(new NotificationRequestConfig())
                .originatorEntityId(ctx.getSelf().getRuleChainId())
                .build();

        var jnksIotMsg = ackIfNeeded(ctx, msg);

        var callback = new FutureCallback<NotificationRequestStats>() {
            @Override
            public void onSuccess(NotificationRequestStats stats) {
                JnksIotMsgMetaData metaData = jnksIotMsg.getMetaData().copy();
                metaData.putValue("notificationRequestResult", JacksonUtil.toString(stats));
                tellSuccess(ctx, jnksIotMsg.transform()
                        .metaData(metaData)
                        .build());
            }

            @Override
            public void onFailure(Throwable e) {
                tellFailure(ctx, jnksIotMsg, e);
            }
        };

        var future = ctx.getNotificationExecutor().executeAsync(() ->
                ctx.getNotificationCenter().processNotificationRequest(ctx.getTenantId(), notificationRequest, callback));
        DonAsynchron.withCallback(future, r -> {}, callback::onFailure);
    }

}
