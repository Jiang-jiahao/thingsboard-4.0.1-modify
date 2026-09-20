package com.jnks.iot.server.service.cf;

import com.google.common.util.concurrent.FutureCallback;
import com.jnks.iot.rule.engine.api.AttributesDeleteRequest;
import com.jnks.iot.rule.engine.api.AttributesSaveRequest;
import com.jnks.iot.rule.engine.api.RuleEngineCalculatedFieldQueueService;
import com.jnks.iot.rule.engine.api.TimeseriesDeleteRequest;
import com.jnks.iot.rule.engine.api.TimeseriesSaveRequest;
import com.jnks.iot.server.common.data.kv.TimeseriesSaveResult;

import java.util.List;

public interface CalculatedFieldQueueService extends RuleEngineCalculatedFieldQueueService {

    void pushRequestToQueue(TimeseriesSaveRequest request, TimeseriesSaveResult result, FutureCallback<Void> callback);

    void pushRequestToQueue(AttributesSaveRequest request, List<Long> result, FutureCallback<Void> callback);

    void pushRequestToQueue(AttributesDeleteRequest request, List<String> result, FutureCallback<Void> callback);

    void pushRequestToQueue(TimeseriesDeleteRequest request, List<String> result, FutureCallback<Void> callback);

}
