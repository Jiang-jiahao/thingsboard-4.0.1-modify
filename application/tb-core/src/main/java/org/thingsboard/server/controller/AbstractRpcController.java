/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.FutureCallback;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.async.DeferredResult;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.exception.ThingsboardErrorCode;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UUIDBased;
import org.thingsboard.server.common.data.rpc.RpcError;
import org.thingsboard.server.common.data.rpc.ToDeviceRpcRequestBody;
import org.thingsboard.server.common.msg.rpc.FromDeviceRpcResponse;
import org.thingsboard.server.common.msg.rpc.ToDeviceRpcRequest;
import org.thingsboard.server.exception.ToErrorResponseEntity;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.rpc.LocalRequestMetaData;
import org.thingsboard.server.service.rpc.TbCoreDeviceRpcService;
import org.thingsboard.server.service.security.AccessValidator;
import org.thingsboard.server.service.security.model.SecurityUser;
import org.thingsboard.server.service.security.permission.Operation;

import java.util.Optional;
import java.util.UUID;

/**
 * 服务端 RPC 的公共实现基类，供 {@code RpcV1Controller} / {@code RpcV2Controller} 复用。
 * <p>
 * 本类本身不暴露 REST 路径。子类收到 HTTP 请求后，把 JSON 体交给
 * {@link #handleDeviceRPCRequest}：先校验当前用户对设备有 {@code RPC_CALL} 权限，
 * 再经 {@link TbCoreDeviceRpcService} 把请求发到设备（或持久化等待上线）。
 * 响应通过 {@link DeferredResult} 异步回写；超时 / 无连接分别映射为调用方指定的 HTTP 状态。
 *
 * @see TbCoreDeviceRpcService
 * @see AccessValidator
 */
@TbCoreComponent
@Slf4j
public abstract class AbstractRpcController extends BaseController {

    /** 把 REST RPC 转发到 Core 设备 RPC 通道（含持久化、重试、设备应答回调）。 */
    @Autowired
    protected TbCoreDeviceRpcService deviceRpcService;

    /** 校验当前用户是否对目标设备有 RPC_CALL 权限。 */
    @Autowired
    protected AccessValidator accessValidator;

    /** REST RPC 超时下限（毫秒），请求里的 timeout 会与此取较大值。 */
    @Value("${server.rest.server_side_rpc.min_timeout:5000}")
    protected long minTimeout;

    /** 请求体未带 timeout 时使用的默认超时（毫秒）。 */
    @Value("${server.rest.server_side_rpc.default_timeout:10000}")
    protected long defaultTimeout;

    /**
     * 解析 RPC JSON（method/params/timeout/expirationTime/persistent/retries 等），
     * 校验权限后交给 {@link TbCoreDeviceRpcService} 下发。
     *
     * @param oneWay {@code true} 为单向 RPC（不等设备应答）；{@code false} 为双向
     */
    protected DeferredResult<ResponseEntity> handleDeviceRPCRequest(boolean oneWay, DeviceId deviceId, String requestBody, HttpStatus timeoutStatus, HttpStatus noActiveConnectionStatus) throws ThingsboardException {
        try {
            JsonNode rpcRequestBody = JacksonUtil.toJsonNode(requestBody);
            ToDeviceRpcRequestBody body = new ToDeviceRpcRequestBody(rpcRequestBody.get("method").asText(), JacksonUtil.toString(rpcRequestBody.get("params")));
            SecurityUser currentUser = getCurrentUser();
            TenantId tenantId = currentUser.getTenantId();
            final DeferredResult<ResponseEntity> response = new DeferredResult<>();
            long timeout = rpcRequestBody.has(DataConstants.TIMEOUT) ? rpcRequestBody.get(DataConstants.TIMEOUT).asLong() : defaultTimeout;
            long expTime = rpcRequestBody.has(DataConstants.EXPIRATION_TIME) ? rpcRequestBody.get(DataConstants.EXPIRATION_TIME).asLong() : System.currentTimeMillis() + Math.max(minTimeout, timeout);
            UUID rpcRequestUUID = rpcRequestBody.has("requestUUID") ? UUID.fromString(rpcRequestBody.get("requestUUID").asText()) : UUID.randomUUID();
            boolean persisted = rpcRequestBody.has(DataConstants.PERSISTENT) && rpcRequestBody.get(DataConstants.PERSISTENT).asBoolean();
            String additionalInfo =  JacksonUtil.toString(rpcRequestBody.get(DataConstants.ADDITIONAL_INFO));
            Integer retries = rpcRequestBody.has(DataConstants.RETRIES) ? rpcRequestBody.get(DataConstants.RETRIES).asInt() : null;
            accessValidator.validate(currentUser, Operation.RPC_CALL, deviceId, new HttpValidationCallback(response, new FutureCallback<>() {
                @Override
                public void onSuccess(@Nullable DeferredResult<ResponseEntity> result) {
                    ToDeviceRpcRequest rpcRequest = new ToDeviceRpcRequest(rpcRequestUUID,
                            tenantId,
                            deviceId,
                            oneWay,
                            expTime,
                            body,
                            persisted,
                            retries,
                            additionalInfo
                    );
                    deviceRpcService.processRestApiRpcRequest(rpcRequest, fromDeviceRpcResponse -> reply(new LocalRequestMetaData(rpcRequest, currentUser, result), fromDeviceRpcResponse, timeoutStatus, noActiveConnectionStatus), currentUser);
                }

                @Override
                public void onFailure(Throwable e) {
                    ResponseEntity entity;
                    if (e instanceof ToErrorResponseEntity) {
                        entity = ((ToErrorResponseEntity) e).toErrorResponseEntity();
                    } else {
                        entity = new ResponseEntity(HttpStatus.UNAUTHORIZED);
                    }
                    logRpcCall(currentUser, deviceId, body, oneWay, Optional.empty(), e);
                    response.setResult(entity);
                }
            }));
            return response;
        } catch (IllegalArgumentException ioe) {
            throw new ThingsboardException("Invalid request body", ioe, ThingsboardErrorCode.BAD_REQUEST_PARAMS);
        }
    }

    /**
     * 把设备侧 RPC 应答写成 HTTP 结果：TIMEOUT / NO_ACTIVE_CONNECTION 用调用方传入的状态码；
     * 成功则解析 JSON 返回 200。同时写审计日志。
     */
    public void reply(LocalRequestMetaData rpcRequest, FromDeviceRpcResponse response, HttpStatus timeoutStatus, HttpStatus noActiveConnectionStatus) {
        Optional<RpcError> rpcError = response.getError();
        DeferredResult<ResponseEntity> responseWriter = rpcRequest.getResponseWriter();
        if (rpcError.isPresent()) {
            logRpcCall(rpcRequest, rpcError, null);
            RpcError error = rpcError.get();
            switch (error) {
                case TIMEOUT -> responseWriter.setResult(new ResponseEntity<>(timeoutStatus));
                case NO_ACTIVE_CONNECTION -> responseWriter.setResult(new ResponseEntity<>(noActiveConnectionStatus));
                default -> responseWriter.setResult(new ResponseEntity<>(timeoutStatus));
            }
        } else {
            Optional<String> responseData = response.getResponse();
            if (responseData.isPresent() && !StringUtils.isEmpty(responseData.get())) {
                String data = responseData.get();
                try {
                    logRpcCall(rpcRequest, rpcError, null);
                    responseWriter.setResult(new ResponseEntity<>(JacksonUtil.toJsonNode(data), HttpStatus.OK));
                } catch (IllegalArgumentException e) {
                    log.debug("Failed to decode device response: {}", data, e);
                    logRpcCall(rpcRequest, rpcError, e);
                    responseWriter.setResult(new ResponseEntity<>(HttpStatus.NOT_ACCEPTABLE));
                }
            } else {
                logRpcCall(rpcRequest, rpcError, null);
                responseWriter.setResult(new ResponseEntity<>(HttpStatus.OK));
            }
        }
    }

    private void logRpcCall(LocalRequestMetaData rpcRequest, Optional<RpcError> rpcError, Throwable e) {
        logRpcCall(rpcRequest.getUser(), rpcRequest.getRequest().getDeviceId(), rpcRequest.getRequest().getBody(), rpcRequest.getRequest().isOneway(), rpcError, null);
    }


    private void logRpcCall(SecurityUser user, EntityId entityId, ToDeviceRpcRequestBody body, boolean oneWay, Optional<RpcError> rpcError, Throwable e) {
        String rpcErrorStr = "";
        if (rpcError.isPresent()) {
            rpcErrorStr = "RPC Error: " + rpcError.get().name();
        }
        String method = body.getMethod();
        String params = body.getParams();

        auditLogService.logEntityAction(
                user.getTenantId(),
                user.getCustomerId(),
                user.getId(),
                user.getName(),
                (UUIDBased & EntityId) entityId,
                null,
                ActionType.RPC_CALL,
                BaseController.toException(e),
                rpcErrorStr,
                oneWay,
                method,
                params);
    }


}
