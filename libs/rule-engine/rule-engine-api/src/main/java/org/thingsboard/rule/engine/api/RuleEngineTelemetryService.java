package org.thingsboard.rule.engine.api;

/**
 * Created by ashvayka on 02.04.18.
 */
public interface RuleEngineTelemetryService {

    void saveTimeseries(TimeseriesSaveRequest request);

    void saveAttributes(AttributesSaveRequest request);

    void deleteTimeseries(TimeseriesDeleteRequest request);

    void deleteAttributes(AttributesDeleteRequest request);

}
