package com.jnks.iot.script.api.tbel;

import com.jnks.iot.script.api.ScriptInvokeService;
import com.jnks.iot.server.common.data.script.ScriptLanguage;

public interface TbelInvokeService extends ScriptInvokeService {

    @Override
    default ScriptLanguage getLanguage() {
        return ScriptLanguage.TBEL;
    }

}
