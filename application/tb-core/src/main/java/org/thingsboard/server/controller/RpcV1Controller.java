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

import io.swagger.v3.oas.annotations.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.queue.util.TbCoreComponent;

import java.util.UUID;

import static org.thingsboard.server.controller.ControllerConstants.DEVICE_ID_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH;

/**
 * 设备服务端 RPC v1 REST 入口（已废弃，请用 {@link RpcV2Controller}）。
 * <p>
 * <b>职责：</b>向设备发送单向 / 双向 RPC。超时返回 408，无活动连接返回 409。
 * <p>
 * <b>URL：</b>{@code /api/plugins/rpc}（{@link TbUrlConstants#RPC_V1_URL_PREFIX}）
 * <p>
 * <b>权限：</b>{@code SYS_ADMIN}、{@code TENANT_ADMIN} 或 {@code CUSTOMER_USER}。
 * 实际发送前由父类校验设备 {@code RPC_CALL} 权限。
 * <p>
 * <b>下游：</b>{@link AbstractRpcController#handleDeviceRPCRequest} → {@code TbCoreDeviceRpcService}
 */
@RestController
@TbCoreComponent
@RequestMapping(TbUrlConstants.RPC_V1_URL_PREFIX)
@Slf4j
public class RpcV1Controller extends AbstractRpcController {

    /**
     * 向设备发送单向 RPC（不等待设备响应）。已废弃，请用 v2。
     * <p>
     * 权限：{@code SYS_ADMIN} / {@code TENANT_ADMIN} / {@code CUSTOMER_USER}。
     */
    @ApiOperation(value = "Send one-way RPC request (handleOneWayDeviceRPCRequest)", notes = "Deprecated. See 'Rpc V 2 Controller' instead." + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/oneway/{deviceId}", method = RequestMethod.POST)
    @ResponseBody
    public DeferredResult<ResponseEntity> handleOneWayDeviceRPCRequest(
            @Parameter(description = DEVICE_ID_PARAM_DESCRIPTION)
            @PathVariable("deviceId") String deviceIdStr,
            @Parameter(description = "A JSON value representing the RPC request.")
            @RequestBody String requestBody) throws ThingsboardException {
        return handleDeviceRPCRequest(true, new DeviceId(UUID.fromString(deviceIdStr)), requestBody, HttpStatus.REQUEST_TIMEOUT, HttpStatus.CONFLICT);
    }

    /**
     * 向设备发送双向 RPC（等待设备响应）。已废弃，请用 v2。
     * <p>
     * 权限：{@code SYS_ADMIN} / {@code TENANT_ADMIN} / {@code CUSTOMER_USER}。
     */
    @ApiOperation(value = "Send two-way RPC request (handleTwoWayDeviceRPCRequest)", notes = "Deprecated. See 'Rpc V 2 Controller' instead." + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/twoway/{deviceId}", method = RequestMethod.POST)
    @ResponseBody
    public DeferredResult<ResponseEntity> handleTwoWayDeviceRPCRequest(
            @Parameter(description = DEVICE_ID_PARAM_DESCRIPTION)
            @PathVariable("deviceId") String deviceIdStr,
            @Parameter(description = "A JSON value representing the RPC request.")
            @RequestBody String requestBody) throws ThingsboardException {
        return handleDeviceRPCRequest(false, new DeviceId(UUID.fromString(deviceIdStr)), requestBody, HttpStatus.REQUEST_TIMEOUT, HttpStatus.CONFLICT);
    }

}
