package com.jnks.iot.server.service.rpc;

import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponse;
import com.jnks.iot.server.common.msg.rpc.RemoveRpcActorMsg;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequest;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequestActorMsg;
import com.jnks.iot.server.service.security.model.SecurityUser;

import java.util.function.Consumer;

/**
 * Handles REST API calls that contain RPC requests to Device.
 */
public interface JnksIotCoreDeviceRpcService {

    /**
     * Handles REST API calls that contain RPC requests to Device and pushes them to Rule Engine.
     * Schedules the timeout for the RPC call based on the {@link ToDeviceRpcRequest}
     *  @param request          the RPC request
     * @param responseConsumer the consumer of the RPC response
     * @param currentUser
     */
    void processRestApiRpcRequest(ToDeviceRpcRequest request, Consumer<FromDeviceRpcResponse> responseConsumer, SecurityUser currentUser);

    /**
     * Handles the RPC response from the Rule Engine.
     *
     * @param response the RPC response
     */
    void processRpcResponseFromRuleEngine(FromDeviceRpcResponse response);

    /**
     * Forwards the RPC request from Rule Engine to Device Actor
     *
     * @param request the RPC request message
     */
    void forwardRpcRequestToDeviceActor(ToDeviceRpcRequestActorMsg request);

    /**
     * Handles the RPC response from the Device Actor (Transport).
     *
     * @param response the RPC response
     */
    void processRpcResponseFromDeviceActor(FromDeviceRpcResponse response);

    void processRemoveRpc(RemoveRpcActorMsg removeRpcMsg);

}
