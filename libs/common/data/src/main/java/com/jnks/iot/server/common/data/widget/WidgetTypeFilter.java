package com.jnks.iot.server.common.data.widget;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.List;

@Data
@Builder
public class WidgetTypeFilter {

    private TenantId tenantId;
    private boolean fullSearch;
    private boolean scadaFirst;
    DeprecatedFilter deprecatedFilter;
    List<String> widgetTypes;

}
