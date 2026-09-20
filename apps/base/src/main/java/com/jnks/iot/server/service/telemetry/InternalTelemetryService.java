package com.jnks.iot.server.service.telemetry;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.rule.engine.api.AttributesDeleteRequest;
import com.jnks.iot.rule.engine.api.AttributesSaveRequest;
import com.jnks.iot.rule.engine.api.RuleEngineTelemetryService;
import com.jnks.iot.rule.engine.api.TimeseriesDeleteRequest;
import com.jnks.iot.rule.engine.api.TimeseriesSaveRequest;
import com.jnks.iot.server.common.data.kv.TimeseriesSaveResult;

/**
 * Created by ashvayka on 27.03.18.
 */
public interface InternalTelemetryService extends RuleEngineTelemetryService {

    ListenableFuture<TimeseriesSaveResult> saveTimeseriesInternal(TimeseriesSaveRequest request);

    void saveAttributesInternal(AttributesSaveRequest request);

    void deleteTimeseriesInternal(TimeseriesDeleteRequest request);

    void deleteAttributesInternal(AttributesDeleteRequest request);

}
