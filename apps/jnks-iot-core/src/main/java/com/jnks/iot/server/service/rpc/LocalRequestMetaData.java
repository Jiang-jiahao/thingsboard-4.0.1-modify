package com.jnks.iot.server.service.rpc;

import lombok.Data;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.async.DeferredResult;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequest;
import com.jnks.iot.server.service.security.model.SecurityUser;

/**
 * 本机 REST RPC 请求上下文：原始请求、当前用户与异步 HTTP 结果写入器。
 */
@Data
public class LocalRequestMetaData {
    private final ToDeviceRpcRequest request;
    private final SecurityUser user;
    private final DeferredResult<ResponseEntity> responseWriter;
}
