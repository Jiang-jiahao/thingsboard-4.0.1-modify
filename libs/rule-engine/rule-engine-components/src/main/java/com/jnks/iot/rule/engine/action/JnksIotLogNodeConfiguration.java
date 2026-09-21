package com.jnks.iot.rule.engine.action;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.script.ScriptLanguage;

@Data
public class JnksIotLogNodeConfiguration implements NodeConfiguration {

    private ScriptLanguage scriptLang;
    private String jsScript;
    private String tbelScript;

    @Override
    public JnksIotLogNodeConfiguration defaultConfiguration() {
        JnksIotLogNodeConfiguration configuration = new JnksIotLogNodeConfiguration();
        configuration.setScriptLang(ScriptLanguage.TBEL);
        configuration.setJsScript("return '\\nIncoming message:\\n' + JSON.stringify(msg) + '\\nIncoming metadata:\\n' + JSON.stringify(metadata);");
        configuration.setTbelScript("return '\\nIncoming message:\\n' + JSON.stringify(msg) + '\\nIncoming metadata:\\n' + JSON.stringify(metadata);");
        return configuration;
    }
}
