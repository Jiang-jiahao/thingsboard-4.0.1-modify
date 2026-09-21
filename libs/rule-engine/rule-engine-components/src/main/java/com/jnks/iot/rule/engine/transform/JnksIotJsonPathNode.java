package com.jnks.iot.rule.engine.transform;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "json path",
        configClazz = JnksIotJsonPathNodeConfiguration.class,
        nodeDescription = "Transforms incoming message body using JSONPath expression.",
        nodeDetails = "JSONPath expression specifies a path to an element or a set of elements in a JSON structure.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        icon = "functions",
        configDirective = "jnksIotTransformationNodeJsonPathConfig"
)
public class JnksIotJsonPathNode implements JnksIotNode {

    private JnksIotJsonPathNodeConfiguration config;
    private Configuration configurationJsonPath;
    private JsonPath jsonPath;
    private String jsonPathValue;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotJsonPathNodeConfiguration.class);
        this.jsonPathValue = config.getJsonPath();
        if (!JnksIotJsonPathNodeConfiguration.DEFAULT_JSON_PATH.equals(this.jsonPathValue)) {
            this.configurationJsonPath = Configuration.builder()
                    .jsonProvider(new JacksonJsonNodeJsonProvider())
                    .build();
            this.jsonPath = JsonPath.compile(config.getJsonPath());
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException {
        if (!JnksIotJsonPathNodeConfiguration.DEFAULT_JSON_PATH.equals(this.jsonPathValue)) {
            try {
                Object jsonPathData = jsonPath.read(msg.getData(), this.configurationJsonPath);
                ctx.tellSuccess(msg.transform()
                        .data(JacksonUtil.toString(jsonPathData))
                        .build());
            } catch (PathNotFoundException e) {
                ctx.tellFailure(msg, e);
            }
        } else {
            ctx.tellSuccess(msg);
        }
    }
}
