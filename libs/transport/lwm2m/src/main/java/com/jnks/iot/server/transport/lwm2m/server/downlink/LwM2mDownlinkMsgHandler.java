package com.jnks.iot.server.transport.lwm2m.server.downlink;

import org.eclipse.leshan.core.request.CreateRequest;
import org.eclipse.leshan.core.request.DeleteRequest;
import org.eclipse.leshan.core.request.DiscoverRequest;
import org.eclipse.leshan.core.request.ExecuteRequest;
import org.eclipse.leshan.core.request.ObserveCompositeRequest;
import org.eclipse.leshan.core.request.ObserveRequest;
import org.eclipse.leshan.core.request.ReadCompositeRequest;
import org.eclipse.leshan.core.request.ReadRequest;
import org.eclipse.leshan.core.request.WriteAttributesRequest;
import org.eclipse.leshan.core.request.WriteCompositeRequest;
import org.eclipse.leshan.core.request.WriteRequest;
import org.eclipse.leshan.core.response.CreateResponse;
import org.eclipse.leshan.core.response.DeleteResponse;
import org.eclipse.leshan.core.response.DiscoverResponse;
import org.eclipse.leshan.core.response.ExecuteResponse;
import org.eclipse.leshan.core.response.ObserveCompositeResponse;
import org.eclipse.leshan.core.response.ObserveResponse;
import org.eclipse.leshan.core.response.ReadCompositeResponse;
import org.eclipse.leshan.core.response.ReadResponse;
import org.eclipse.leshan.core.response.WriteAttributesResponse;
import org.eclipse.leshan.core.response.WriteCompositeResponse;
import org.eclipse.leshan.core.response.WriteResponse;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.downlink.composite.JnksIotLwM2MCancelObserveCompositeRequest;
import com.jnks.iot.server.transport.lwm2m.server.downlink.composite.JnksIotLwM2MObserveCompositeRequest;
import com.jnks.iot.server.transport.lwm2m.server.downlink.composite.JnksIotLwM2MReadCompositeRequest;
import com.jnks.iot.server.transport.lwm2m.server.rpc.composite.RpcWriteCompositeRequest;

import java.util.List;
import java.util.Set;

public interface LwM2mDownlinkMsgHandler {

    void sendReadRequest(LwM2mClient client, JnksIotLwM2MReadRequest request, DownlinkRequestCallback<ReadRequest, ReadResponse> callback);

    void sendReadCompositeRequest(LwM2mClient client, JnksIotLwM2MReadCompositeRequest request, DownlinkRequestCallback<ReadCompositeRequest, ReadCompositeResponse> callback);

    void sendObserveRequest(LwM2mClient client, JnksIotLwM2MObserveRequest request, DownlinkRequestCallback<ObserveRequest, ObserveResponse> callback);

    void sendObserveAllRequest(LwM2mClient client, JnksIotLwM2MObserveAllRequest request, DownlinkRequestCallback<JnksIotLwM2MObserveAllRequest, Set<String>> callback);

    void sendExecuteRequest(LwM2mClient client, JnksIotLwM2MExecuteRequest request, DownlinkRequestCallback<ExecuteRequest, ExecuteResponse> callback);

    void sendDeleteRequest(LwM2mClient client, JnksIotLwM2MDeleteRequest request, DownlinkRequestCallback<DeleteRequest, DeleteResponse> callback);

    void sendCancelObserveRequest(LwM2mClient client, JnksIotLwM2MCancelObserveRequest request, DownlinkRequestCallback<JnksIotLwM2MCancelObserveRequest, Integer> callback);

    void sendCancelObserveAllRequest(LwM2mClient client, JnksIotLwM2MCancelAllRequest request, DownlinkRequestCallback<JnksIotLwM2MCancelAllRequest, Integer> callback);

    void sendObserveCompositeRequest(LwM2mClient client, JnksIotLwM2MObserveCompositeRequest request, DownlinkRequestCallback<ObserveCompositeRequest, ObserveCompositeResponse> callback);

    void sendCancelObserveCompositeRequest(LwM2mClient client, JnksIotLwM2MCancelObserveCompositeRequest request, DownlinkRequestCallback<JnksIotLwM2MCancelObserveCompositeRequest, Integer> callback);

    void sendDiscoverRequest(LwM2mClient client, JnksIotLwM2MDiscoverRequest request, DownlinkRequestCallback<DiscoverRequest, DiscoverResponse> callback);

    void sendDiscoverAllRequest(LwM2mClient client, JnksIotLwM2MDiscoverAllRequest request, DownlinkRequestCallback<JnksIotLwM2MDiscoverAllRequest, List<String>> callback);

    void sendWriteAttributesRequest(LwM2mClient client, JnksIotLwM2MWriteAttributesRequest request, DownlinkRequestCallback<WriteAttributesRequest, WriteAttributesResponse> callback);

    void sendWriteReplaceRequest(LwM2mClient client, JnksIotLwM2MWriteReplaceRequest request, DownlinkRequestCallback<WriteRequest, WriteResponse> callback);

    void sendWriteCompositeRequest(LwM2mClient client, RpcWriteCompositeRequest nodes, DownlinkRequestCallback<WriteCompositeRequest, WriteCompositeResponse> callback);

    void sendWriteUpdateRequest(LwM2mClient client, JnksIotLwM2MWriteUpdateRequest request, DownlinkRequestCallback<WriteRequest, WriteResponse> callback);

    void sendCreateRequest(LwM2mClient client, JnksIotLwM2MCreateRequest request, DownlinkRequestCallback<CreateRequest, CreateResponse> callback);

}
