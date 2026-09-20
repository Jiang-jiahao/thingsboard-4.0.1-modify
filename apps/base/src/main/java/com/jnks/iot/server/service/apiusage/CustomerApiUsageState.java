package com.jnks.iot.server.service.apiusage;

import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.EntityType;

public class CustomerApiUsageState extends BaseApiUsageState {
    public CustomerApiUsageState(ApiUsageState apiUsageState) {
        super(apiUsageState);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.CUSTOMER;
    }
}
