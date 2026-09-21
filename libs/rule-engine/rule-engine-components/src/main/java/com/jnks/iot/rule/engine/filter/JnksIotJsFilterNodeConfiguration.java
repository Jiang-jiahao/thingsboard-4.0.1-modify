package com.jnks.iot.rule.engine.filter;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.script.ScriptLanguage;

@Data
public class JnksIotJsFilterNodeConfiguration implements NodeConfiguration<JnksIotJsFilterNodeConfiguration> {

    private ScriptLanguage scriptLang;
    private String jsScript;
    private String tbelScript;

    @Override
    public JnksIotJsFilterNodeConfiguration defaultConfiguration() {
        JnksIotJsFilterNodeConfiguration configuration = new JnksIotJsFilterNodeConfiguration();
        configuration.setScriptLang(ScriptLanguage.TBEL);
        configuration.setJsScript("return msg.temperature > 20;");
        configuration.setTbelScript("return msg.temperature > 20;");
        return configuration;
    }
}
