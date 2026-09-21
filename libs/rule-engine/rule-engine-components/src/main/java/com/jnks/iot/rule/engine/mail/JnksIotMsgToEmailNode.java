package com.jnks.iot.rule.engine.mail;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotEmail;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "转为邮件",
        configClazz = JnksIotMsgToEmailNodeConfiguration.class,
        nodeDescription = "将消息转换为邮件消息",
        nodeDetails = "将消息转换为邮件消息。若转换成功完成，输出消息类型将设置为 <code>SEND_EMAIL</code>。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotTransformationNodeToEmailConfig",
        icon = "email"
)
public class JnksIotMsgToEmailNode implements JnksIotNode {

    private static final String IMAGES = "images";
    private static final String DYNAMIC = "dynamic";

    private JnksIotMsgToEmailNodeConfiguration config;
    private boolean dynamicMailBodyType;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotMsgToEmailNodeConfiguration.class);
        this.dynamicMailBodyType = DYNAMIC.equals(this.config.getMailBodyType());
     }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        try {
            JnksIotEmail email = convert(msg);
            JnksIotMsg emailMsg = buildEmailMsg(ctx, msg, email);
            ctx.tellNext(emailMsg, JnksIotNodeConnectionType.SUCCESS);
        } catch (Exception ex) {
            log.warn("Can not convert message to email " + ex.getMessage());
            ctx.tellFailure(msg, ex);
        }
    }

    private JnksIotMsg buildEmailMsg(JnksIotContext ctx, JnksIotMsg msg, JnksIotEmail email) {
        String emailJson = JacksonUtil.toString(email);
        return ctx.transformMsg(msg, JnksIotMsgType.SEND_EMAIL, msg.getOriginator(), msg.getMetaData().copy(), emailJson);
    }

    private JnksIotEmail convert(JnksIotMsg msg) {
        JnksIotEmail.JnksIotEmailBuilder builder = JnksIotEmail.builder();
        builder.from(fromTemplate(config.getFromTemplate(), msg));
        builder.to(fromTemplate(config.getToTemplate(), msg));
        builder.cc(fromTemplate(config.getCcTemplate(), msg));
        builder.bcc(fromTemplate(config.getBccTemplate(), msg));
        String htmlStr = dynamicMailBodyType ?
                fromTemplate(config.getIsHtmlTemplate(), msg) : config.getMailBodyType();
        builder.html(Boolean.parseBoolean(htmlStr));
        builder.subject(fromTemplate(config.getSubjectTemplate(), msg));
        builder.body(fromTemplate(config.getBodyTemplate(), msg));
        String imagesStr = msg.getMetaData().getValue(IMAGES);
        if (!StringUtils.isEmpty(imagesStr)) {
            Map<String, String> imgMap = JacksonUtil.fromString(imagesStr, new TypeReference<HashMap<String, String>>() {});
            builder.images(imgMap);
        }
        return builder.build();
    }

    private String fromTemplate(String template, JnksIotMsg msg) {
        return StringUtils.isNotEmpty(template) ? JnksIotNodeUtils.processPattern(template, msg) : null;
    }

}
