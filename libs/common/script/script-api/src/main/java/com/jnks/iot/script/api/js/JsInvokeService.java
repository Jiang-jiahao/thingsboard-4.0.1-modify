package com.jnks.iot.script.api.js;

import com.jnks.iot.script.api.ScriptInvokeService;
import com.jnks.iot.server.common.data.script.ScriptLanguage;

public interface JsInvokeService extends ScriptInvokeService {

    @Override
    default ScriptLanguage getLanguage() {
        return ScriptLanguage.JS;
    }

}
