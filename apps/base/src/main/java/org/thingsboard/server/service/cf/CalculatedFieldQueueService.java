package org.thingsboard.server.service.cf;

import com.google.common.util.concurrent.FutureCallback;
import org.thingsboard.rule.engine.api.AttributesDeleteRequest;
import org.thingsboard.rule.engine.api.AttributesSaveRequest;
import org.thingsboard.rule.engine.api.RuleEngineCalculatedFieldQueueService;
import org.thingsboard.rule.engine.api.TimeseriesDeleteRequest;
import org.thingsboard.rule.engine.api.TimeseriesSaveRequest;
import org.thingsboard.server.common.data.kv.TimeseriesSaveResult;

import java.util.List;

public interface CalculatedFieldQueueService extends RuleEngineCalculatedFieldQueueService {

    void pushRequestToQueue(TimeseriesSaveRequest request, TimeseriesSaveResult result, FutureCallback<Void> callback);

    void pushRequestToQueue(AttributesSaveRequest request, List<Long> result, FutureCallback<Void> callback);

    void pushRequestToQueue(AttributesDeleteRequest request, List<String> result, FutureCallback<Void> callback);

    void pushRequestToQueue(TimeseriesDeleteRequest request, List<String> result, FutureCallback<Void> callback);

}
