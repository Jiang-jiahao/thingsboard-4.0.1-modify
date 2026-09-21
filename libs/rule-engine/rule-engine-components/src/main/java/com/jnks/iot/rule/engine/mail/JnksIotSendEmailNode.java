package com.jnks.iot.rule.engine.mail;

import lombok.extern.slf4j.Slf4j;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotEmail;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.external.JnksIotAbstractExternalNode;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.io.IOException;
import java.util.Properties;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;

@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "send email",
        configClazz = JnksIotSendEmailNodeConfiguration.class,
        nodeDescription = "Sends email message via SMTP server.",
        nodeDetails = "Expects messages with <b>SEND_EMAIL</b> type. Node works only with messages that " +
                " where created using <code>to Email</code> transformation Node, please connect this Node " +
                "with <code>to Email</code> Node using <code>Successful</code> chain.",
        configDirective = "jnksIotExternalNodeSendEmailConfig",
        icon = "send"
)
public class JnksIotSendEmailNode extends JnksIotAbstractExternalNode {

    private static final String MAIL_PROP = "mail.";
    private JnksIotSendEmailNodeConfiguration config;
    private JavaMailSenderImpl mailSender;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        super.init(ctx);
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotSendEmailNodeConfiguration.class);
        try {
            if (!this.config.isUseSystemSmtpSettings()) {
                mailSender = createMailSender();
            }
        } catch (Exception e) {
            throw new JnksIotNodeException(e);
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        try {
            validateType(msg);
            JnksIotEmail email = getEmail(msg);
            var jnksIotMsg = ackIfNeeded(ctx, msg);
            withCallback(ctx.getMailExecutor().executeAsync(() -> {
                        sendEmail(ctx, jnksIotMsg, email);
                        return null;
                    }),
                    ok -> tellSuccess(ctx, jnksIotMsg),
                    fail -> tellFailure(ctx, jnksIotMsg, fail));
        } catch (Exception ex) {
            ctx.tellFailure(msg, ex);
        }
    }

    private void sendEmail(JnksIotContext ctx, JnksIotMsg msg, JnksIotEmail email) throws Exception {
        if (this.config.isUseSystemSmtpSettings()) {
            ctx.getMailService(true).send(ctx.getTenantId(), msg.getCustomerId(), email);
        } else {
            ctx.getMailService(false).send(ctx.getTenantId(), msg.getCustomerId(), email, this.mailSender, config.getTimeout());
        }
    }

    private JnksIotEmail getEmail(JnksIotMsg msg) throws IOException {
        JnksIotEmail email = JacksonUtil.fromString(msg.getData(), JnksIotEmail.class);
        if (StringUtils.isBlank(email.getTo())) {
            throw new IllegalStateException("Email destination can not be blank [" + email.getTo() + "]");
        }
        return email;
    }

    private void validateType(JnksIotMsg msg) {
        if (!msg.isTypeOf(JnksIotMsgType.SEND_EMAIL)) {
            String type = msg.getType();
            log.warn("Not expected msg type [{}] for SendEmail Node", type);
            throw new IllegalStateException("Not expected msg type " + type + " for SendEmail Node");
        }
    }

    private JavaMailSenderImpl createMailSender() {
        JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
        mailSender.setHost(this.config.getSmtpHost());
        mailSender.setPort(this.config.getSmtpPort());
        mailSender.setUsername(this.config.getUsername());
        mailSender.setPassword(this.config.getPassword());
        mailSender.setJavaMailProperties(createJavaMailProperties());
        return mailSender;
    }

    private Properties createJavaMailProperties() {
        Properties javaMailProperties = new Properties();
        String protocol = this.config.getSmtpProtocol();
        javaMailProperties.put("mail.transport.protocol", protocol);
        javaMailProperties.put(MAIL_PROP + protocol + ".host", this.config.getSmtpHost());
        javaMailProperties.put(MAIL_PROP + protocol + ".port", this.config.getSmtpPort() + "");
        javaMailProperties.put(MAIL_PROP + protocol + ".timeout", this.config.getTimeout() + "");
        javaMailProperties.put(MAIL_PROP + protocol + ".auth", String.valueOf(StringUtils.isNotEmpty(this.config.getUsername())));
        javaMailProperties.put(MAIL_PROP + protocol + ".starttls.enable", Boolean.valueOf(this.config.isEnableTls()).toString());
        if (this.config.isEnableTls() && StringUtils.isNoneEmpty(this.config.getTlsVersion())) {
            javaMailProperties.put(MAIL_PROP + protocol + ".ssl.protocols", this.config.getTlsVersion());
        }
        if (this.config.isEnableProxy()) {
            javaMailProperties.put(MAIL_PROP + protocol + ".proxy.host", config.getProxyHost());
            javaMailProperties.put(MAIL_PROP + protocol + ".proxy.port", config.getProxyPort());
            if (StringUtils.isNoneEmpty(config.getProxyUser())) {
                javaMailProperties.put(MAIL_PROP + protocol + ".proxy.user", config.getProxyUser());
            }
            if (StringUtils.isNoneEmpty(config.getProxyPassword())) {
                javaMailProperties.put(MAIL_PROP + protocol + ".proxy.password", config.getProxyPassword());
            }
        }
        return javaMailProperties;
    }
}
