package com.jnks.iot.rule.engine.action;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.data.validation.NoXss;

import java.util.Collections;
import java.util.List;

@Data
public class JnksIotCreateAlarmNodeConfiguration extends JnksIotAbstractAlarmNodeConfiguration implements NodeConfiguration<JnksIotCreateAlarmNodeConfiguration> {

    @NoXss
    private String severity;
    private boolean propagate;
    private boolean propagateToOwner;
    private boolean propagateToTenant;
    private boolean useMessageAlarmData;
    private boolean overwriteAlarmDetails = true;
    private boolean dynamicSeverity;

    private List<String> relationTypes;

    @Override
    public JnksIotCreateAlarmNodeConfiguration defaultConfiguration() {
        JnksIotCreateAlarmNodeConfiguration configuration = new JnksIotCreateAlarmNodeConfiguration();
        configuration.setScriptLang(ScriptLanguage.TBEL);
        configuration.setAlarmDetailsBuildJs(ALARM_DETAILS_BUILD_JS_TEMPLATE);
        configuration.setAlarmDetailsBuildTbel(ALARM_DETAILS_BUILD_TBEL_TEMPLATE);
        configuration.setAlarmType("General Alarm");
        configuration.setSeverity(AlarmSeverity.CRITICAL.name());
        configuration.setPropagate(false);
        configuration.setPropagateToOwner(false);
        configuration.setPropagateToTenant(false);
        configuration.setUseMessageAlarmData(false);
        configuration.setOverwriteAlarmDetails(false);
        configuration.setRelationTypes(Collections.emptyList());
        configuration.setDynamicSeverity(false);
        return configuration;
    }

}
