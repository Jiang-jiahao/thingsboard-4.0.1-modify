package com.jnks.iot.server.transport.lwm2m.server.rpc.composite;

import org.eclipse.leshan.core.request.LwM2mRequest;
import org.eclipse.leshan.core.response.ReadCompositeResponse;
import com.jnks.iot.server.common.transport.TransportService;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.transport.lwm2m.server.client.LwM2mClient;
import com.jnks.iot.server.transport.lwm2m.server.downlink.DownlinkRequestCallback;
import com.jnks.iot.server.transport.lwm2m.server.rpc.RpcLwM2MDownlinkCallback;

import java.util.Optional;

import static com.jnks.iot.server.transport.lwm2m.utils.LwM2MTransportUtil.contentToString;

public class RpcReadResponseCompositeCallback<R extends LwM2mRequest<T>, T extends ReadCompositeResponse> extends RpcLwM2MDownlinkCallback<R, T> {

    public RpcReadResponseCompositeCallback(TransportService transportService, LwM2mClient client, TransportProtos.ToDeviceRpcRequestMsg requestMsg, DownlinkRequestCallback<R, T> callback) {
        super(transportService, client, requestMsg, callback);
    }

    @Override
    protected Optional<String> serializeSuccessfulResponse(T response) {
        return contentToString(response.getContent());
    }
}
