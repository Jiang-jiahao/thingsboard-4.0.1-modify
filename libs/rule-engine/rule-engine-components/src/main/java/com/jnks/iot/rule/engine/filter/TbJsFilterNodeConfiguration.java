package com.jnks.iot.rule.engine.filter;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.script.ScriptLanguage;

@Data
public class TbJsFilterNodeConfiguration implements NodeConfiguration<TbJsFilterNodeConfiguration> {

    private ScriptLanguage scriptLang;
    private String jsScript;
    private String tbelScript;

    @Override
    public TbJsFilterNodeConfiguration defaultConfiguration() {
        TbJsFilterNodeConfiguration configuration = new TbJsFilterNodeConfiguration();
        configuration.setScriptLang(ScriptLanguage.TBEL);
        configuration.setJsScript("return msg.temperature > 20;");
        configuration.setTbelScript("return msg.temperature > 20;");
        return configuration;
    }
}
