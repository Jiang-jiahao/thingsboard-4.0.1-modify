package com.jnks.iot.rule.engine.filter;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.script.ScriptLanguage;

@Data
public class JnksIotJsSwitchNodeConfiguration implements NodeConfiguration<JnksIotJsSwitchNodeConfiguration> {

    private static final String DEFAULT_JS_SCRIPT = "function nextRelation(metadata, msg) {\n" +
            "    return ['one','nine'];\n" +
            "}\n" +
            "if(msgType === 'POST_TELEMETRY_REQUEST') {\n" +
            "    return ['two'];\n" +
            "}\n" +
            "return nextRelation(metadata, msg);";

    private static final String DEFAULT_TBEL_SCRIPT = "function nextRelation(metadata, msg) {\n" +
            "    return ['one','nine'];\n" +
            "}\n" +
            "if(msgType == 'POST_TELEMETRY_REQUEST') {\n" +
            "    return ['two'];\n" +
            "}\n" +
            "return nextRelation(metadata, msg);";

    private ScriptLanguage scriptLang;
    private String jsScript;
    private String tbelScript;

    @Override
    public JnksIotJsSwitchNodeConfiguration defaultConfiguration() {
        JnksIotJsSwitchNodeConfiguration configuration = new JnksIotJsSwitchNodeConfiguration();
        configuration.setScriptLang(ScriptLanguage.TBEL);
        configuration.setJsScript(DEFAULT_JS_SCRIPT);
        configuration.setTbelScript(DEFAULT_TBEL_SCRIPT);
        return configuration;
    }
}
