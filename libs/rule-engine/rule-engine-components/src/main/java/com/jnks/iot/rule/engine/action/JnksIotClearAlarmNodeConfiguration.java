package com.jnks.iot.rule.engine.action;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.script.ScriptLanguage;

@Data
public class JnksIotClearAlarmNodeConfiguration extends JnksIotAbstractAlarmNodeConfiguration implements NodeConfiguration<JnksIotClearAlarmNodeConfiguration> {

    @Override
    public JnksIotClearAlarmNodeConfiguration defaultConfiguration() {
        JnksIotClearAlarmNodeConfiguration configuration = new JnksIotClearAlarmNodeConfiguration();
        configuration.setScriptLang(ScriptLanguage.TBEL);
        configuration.setAlarmDetailsBuildJs(ALARM_DETAILS_BUILD_JS_TEMPLATE);
        configuration.setAlarmDetailsBuildTbel(ALARM_DETAILS_BUILD_TBEL_TEMPLATE);
        configuration.setAlarmType("General Alarm");
        return configuration;
    }
}
