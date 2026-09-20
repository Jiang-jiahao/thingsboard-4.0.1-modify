package com.jnks.iot.server.service.cf.ctx;

import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.service.cf.ctx.state.CalculatedFieldState;

@Data
@NoArgsConstructor
public class CalculatedFieldEntityCtx {

    private CalculatedFieldEntityCtxId id;
    private CalculatedFieldState state;

    public CalculatedFieldEntityCtx(CalculatedFieldEntityCtxId id, CalculatedFieldState state) {
        this.id = id;
        this.state = state;
    }

}
