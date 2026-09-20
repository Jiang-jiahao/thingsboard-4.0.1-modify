package com.jnks.iot.server.common.data.query;

import lombok.Data;
import com.jnks.iot.server.common.data.id.CustomerId;

@Data
public class ApiUsageStateFilter implements EntityFilter {

    private CustomerId customerId;

    @Override
    public EntityFilterType getType() {
        return EntityFilterType.API_USAGE_STATE;
    }

}
