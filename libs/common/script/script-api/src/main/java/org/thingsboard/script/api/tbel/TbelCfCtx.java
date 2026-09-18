package org.thingsboard.script.api.tbel;

import lombok.Getter;

import java.util.Collections;
import java.util.Map;

public class TbelCfCtx implements TbelCfObject {

    @Getter
    private final Map<String, TbelCfArg> args;

    public TbelCfCtx(Map<String, TbelCfArg> args) {
        this.args = Collections.unmodifiableMap(args);
    }

    @Override
    public long memorySize() {
        return OBJ_SIZE;
    }
}
