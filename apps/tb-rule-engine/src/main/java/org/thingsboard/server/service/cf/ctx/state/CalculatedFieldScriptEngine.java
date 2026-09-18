package org.thingsboard.server.service.cf.ctx.state;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * 计算字段脚本执行引擎
 */
public interface CalculatedFieldScriptEngine {

    ListenableFuture<Object> executeScriptAsync(Object[] args);

    ListenableFuture<JsonNode> executeJsonAsync(Object[] args);

    void destroy();

}
