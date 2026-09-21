package com.jnks.iot.rule.engine.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.List;
import java.util.Set;

public interface ScriptEngine {

    ListenableFuture<List<JnksIotMsg>> executeUpdateAsync(JnksIotMsg msg);

    ListenableFuture<JnksIotMsg> executeGenerateAsync(JnksIotMsg prevMsg);

    ListenableFuture<Boolean> executeFilterAsync(JnksIotMsg msg);

    ListenableFuture<Set<String>> executeSwitchAsync(JnksIotMsg msg);

    ListenableFuture<JsonNode> executeJsonAsync(JnksIotMsg msg);

    ListenableFuture<String> executeToStringAsync(JnksIotMsg msg);

    void destroy();

}
