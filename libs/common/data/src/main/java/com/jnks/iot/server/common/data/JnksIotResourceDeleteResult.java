package com.jnks.iot.server.common.data;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.HasId;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class JnksIotResourceDeleteResult {

    private boolean success;
    private Map<String, List<? extends HasId<?>>> references;

}
