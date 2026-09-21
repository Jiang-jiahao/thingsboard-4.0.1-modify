package com.jnks.iot.rule.engine.geo;

import lombok.Data;
import com.jnks.iot.common.util.geo.PerimeterType;

import java.util.concurrent.TimeUnit;

/**
 * Created by ashvayka on 19.01.18.
 */
@Data
public class JnksIotGpsGeofencingActionNodeConfiguration extends JnksIotGpsGeofencingFilterNodeConfiguration {

    private int minInsideDuration;
    private int minOutsideDuration;

    private String minInsideDurationTimeUnit;
    private String minOutsideDurationTimeUnit;

    private boolean reportPresenceStatusOnEachMessage;

    @Override
    public JnksIotGpsGeofencingActionNodeConfiguration defaultConfiguration() {
        JnksIotGpsGeofencingActionNodeConfiguration configuration = new JnksIotGpsGeofencingActionNodeConfiguration();
        configuration.setLatitudeKeyName("latitude");
        configuration.setLongitudeKeyName("longitude");
        configuration.setPerimeterType(PerimeterType.POLYGON);
        configuration.setFetchPerimeterInfoFromMessageMetadata(true);
        configuration.setPerimeterKeyName("ss_perimeter");
        configuration.setMinInsideDurationTimeUnit(TimeUnit.MINUTES.name());
        configuration.setMinOutsideDurationTimeUnit(TimeUnit.MINUTES.name());
        configuration.setMinInsideDuration(1);
        configuration.setMinOutsideDuration(1);
        configuration.setReportPresenceStatusOnEachMessage(true);
        return configuration;
    }
}
