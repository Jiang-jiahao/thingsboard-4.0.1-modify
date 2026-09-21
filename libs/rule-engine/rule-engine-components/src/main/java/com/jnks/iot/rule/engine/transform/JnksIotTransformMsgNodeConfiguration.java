package com.jnks.iot.rule.engine.transform;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.script.ScriptLanguage;

@Data
public class JnksIotTransformMsgNodeConfiguration implements NodeConfiguration<JnksIotTransformMsgNodeConfiguration> {

    private ScriptLanguage scriptLang;
    private String jsScript;
    private String tbelScript;

    @Override
    public JnksIotTransformMsgNodeConfiguration defaultConfiguration() {
        JnksIotTransformMsgNodeConfiguration configuration = new JnksIotTransformMsgNodeConfiguration();
        configuration.setScriptLang(ScriptLanguage.TBEL);
        configuration.setJsScript("return {msg: msg, metadata: metadata, msgType: msgType};");
        configuration.setTbelScript("return {msg: msg, metadata: metadata, msgType: msgType};");
        return configuration;
    }
}
