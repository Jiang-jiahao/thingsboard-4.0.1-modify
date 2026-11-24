package com.jnks.iot.server.service.rpc;

import lombok.Data;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.async.DeferredResult;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequest;
import com.jnks.iot.server.service.security.model.SecurityUser;

/**
 * 本机 REST RPC 请求上下文：原始请求、实际下发请求、当前用户与异步 HTTP 结果写入器。
 */
@Data
public class LocalRequestMetaData {
    private final ToDeviceRpcRequest request;
    /** 合并/组帧后真正下发的请求；与 request 不同时审计记录两者 */
    private final ToDeviceRpcRequest actualRequest;
    private final SecurityUser user;
    private final DeferredResult<ResponseEntity> responseWriter;
}
