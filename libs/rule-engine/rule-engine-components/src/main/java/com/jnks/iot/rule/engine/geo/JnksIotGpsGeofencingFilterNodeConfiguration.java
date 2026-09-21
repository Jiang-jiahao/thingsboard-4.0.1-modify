package com.jnks.iot.rule.engine.geo;

import lombok.Data;
import com.jnks.iot.common.util.geo.PerimeterType;
import com.jnks.iot.common.util.geo.RangeUnit;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

/**
 * Created by ashvayka on 19.01.18.
 */
@Data
public class JnksIotGpsGeofencingFilterNodeConfiguration implements NodeConfiguration<JnksIotGpsGeofencingFilterNodeConfiguration> {

    private String latitudeKeyName;
    private String longitudeKeyName;
    private PerimeterType perimeterType;

    private boolean fetchPerimeterInfoFromMessageMetadata;
    // If Perimeter is fetched from metadata
    private String perimeterKeyName;

    //For Polygons
    private String polygonsDefinition;

    //For Circles
    private Double centerLatitude;
    private Double centerLongitude;
    private Double range;
    private RangeUnit rangeUnit;

    @Override
    public JnksIotGpsGeofencingFilterNodeConfiguration defaultConfiguration() {
        JnksIotGpsGeofencingFilterNodeConfiguration configuration = new JnksIotGpsGeofencingFilterNodeConfiguration();
        configuration.setLatitudeKeyName("latitude");
        configuration.setLongitudeKeyName("longitude");
        configuration.setPerimeterType(PerimeterType.POLYGON);
        configuration.setFetchPerimeterInfoFromMessageMetadata(true);
        configuration.setPerimeterKeyName("ss_perimeter");
        return configuration;
    }
}
