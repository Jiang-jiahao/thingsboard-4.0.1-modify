package com.jnks.iot.rule.engine.mail;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.TbContext;
import com.jnks.iot.rule.engine.api.TbEmail;
import com.jnks.iot.rule.engine.api.TbNode;
import com.jnks.iot.rule.engine.api.TbNodeConfiguration;
import com.jnks.iot.rule.engine.api.TbNodeException;
import com.jnks.iot.rule.engine.api.util.TbNodeUtils;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.msg.TbMsgType;
import com.jnks.iot.server.common.data.msg.TbNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.TbMsg;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "to email",
        configClazz = TbMsgToEmailNodeConfiguration.class,
        nodeDescription = "Transforms message to email message",
        nodeDetails = "Transforms message to email message. If transformation completed successfully output message type will be set to <code>SEND_EMAIL</code>.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "tbTransformationNodeToEmailConfig",
        icon = "email"
)
public class TbMsgToEmailNode implements TbNode {

    private static final String IMAGES = "images";
    private static final String DYNAMIC = "dynamic";

    private TbMsgToEmailNodeConfiguration config;
    private boolean dynamicMailBodyType;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbMsgToEmailNodeConfiguration.class);
        this.dynamicMailBodyType = DYNAMIC.equals(this.config.getMailBodyType());
     }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        try {
            TbEmail email = convert(msg);
            TbMsg emailMsg = buildEmailMsg(ctx, msg, email);
            ctx.tellNext(emailMsg, TbNodeConnectionType.SUCCESS);
        } catch (Exception ex) {
            log.warn("Can not convert message to email " + ex.getMessage());
            ctx.tellFailure(msg, ex);
        }
    }

    private TbMsg buildEmailMsg(TbContext ctx, TbMsg msg, TbEmail email) {
        String emailJson = JacksonUtil.toString(email);
        return ctx.transformMsg(msg, TbMsgType.SEND_EMAIL, msg.getOriginator(), msg.getMetaData().copy(), emailJson);
    }

    private TbEmail convert(TbMsg msg) {
        TbEmail.TbEmailBuilder builder = TbEmail.builder();
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

    private String fromTemplate(String template, TbMsg msg) {
        return StringUtils.isNotEmpty(template) ? TbNodeUtils.processPattern(template, msg) : null;
    }

}
