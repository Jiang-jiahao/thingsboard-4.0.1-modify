package com.jnks.iot.rule.engine.sms;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.sms.SmsSender;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.external.JnksIotAbstractExternalNode;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;

@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "发送短信",
        configClazz = JnksIotSendSmsNodeConfiguration.class,
        nodeDescription = "通过短信服务商发送短信消息。",
        nodeDetails = "将使用从消息元数据中获取的值填充目标电话号码和短信消息字段，从而发送短信。",
        configDirective = "jnksIotExternalNodeSendSmsConfig",
        icon = "sms"
)
public class JnksIotSendSmsNode extends JnksIotAbstractExternalNode {

    private JnksIotSendSmsNodeConfiguration config;
    private SmsSender smsSender;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx);
        try {
            this.config = JnksIotNodeUtils.convert(configuration, JnksIotSendSmsNodeConfiguration.class);
            if (!this.config.isUseSystemSmsSettings()) {
                smsSender = createSmsSender(ctx);
            }
        } catch (Exception e) {
            throw new JnksIotNodeException(e);
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        var jnksIotMsg = ackIfNeeded(ctx, msg);
        try {
            withCallback(ctx.getSmsExecutor().executeAsync(() -> {
                        sendSms(ctx, jnksIotMsg);
                        return null;
                    }),
                    ok -> tellSuccess(ctx, jnksIotMsg),
                    fail -> tellFailure(ctx, jnksIotMsg, fail));
        } catch (Exception ex) {
            ctx.tellFailure(jnksIotMsg, ex);
        }
    }

    private void sendSms(JnksIotContext ctx, JnksIotMsg msg) throws Exception {
        String numbersTo = JnksIotNodeUtils.processPattern(this.config.getNumbersToTemplate(), msg);
        String message = JnksIotNodeUtils.processPattern(this.config.getSmsMessageTemplate(), msg);
        String[] numbersToList = numbersTo.split(",");
        if (this.config.isUseSystemSmsSettings()) {
            ctx.getSmsService().sendSms(ctx.getTenantId(), msg.getCustomerId(), numbersToList, message);
        } else {
            for (String numberTo : numbersToList) {
                this.smsSender.sendSms(numberTo, message);
            }
        }
    }

    @Override
    public void destroy() {
        if (this.smsSender != null) {
            this.smsSender.destroy();
        }
    }

    private SmsSender createSmsSender(JnksIotContext ctx) {
        return ctx.getSmsSenderFactory().createSmsSender(this.config.getSmsProviderConfiguration());
    }

}
