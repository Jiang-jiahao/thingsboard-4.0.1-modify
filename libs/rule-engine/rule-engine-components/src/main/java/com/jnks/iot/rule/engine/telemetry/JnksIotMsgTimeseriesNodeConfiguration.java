package com.jnks.iot.rule.engine.telemetry;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings;

import static com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings.OnEveryMessage;

@Data
public class JnksIotMsgTimeseriesNodeConfiguration implements NodeConfiguration<JnksIotMsgTimeseriesNodeConfiguration> {

    private long defaultTTL;
    private boolean useServerTs;
    @NotNull
    private TimeseriesProcessingSettings processingSettings;

    @Override
    public JnksIotMsgTimeseriesNodeConfiguration defaultConfiguration() {
        JnksIotMsgTimeseriesNodeConfiguration configuration = new JnksIotMsgTimeseriesNodeConfiguration();
        configuration.setDefaultTTL(0L);
        configuration.setUseServerTs(false);
        configuration.setProcessingSettings(new OnEveryMessage());
        return configuration;
    }

}
