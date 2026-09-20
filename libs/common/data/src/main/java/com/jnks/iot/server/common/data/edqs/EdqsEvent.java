package com.jnks.iot.server.common.data.edqs;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.ObjectType;
import com.jnks.iot.server.common.data.id.TenantId;

@Data
@AllArgsConstructor
@Builder
public class EdqsEvent {
    
    private final TenantId tenantId;
    private final ObjectType objectType;
    private final EdqsEventType eventType;
    private final EdqsObject object;

}
