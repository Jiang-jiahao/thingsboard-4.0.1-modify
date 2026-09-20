package com.jnks.iot.server.edqs.query.processor;

import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.permission.QueryContext;
import com.jnks.iot.server.common.data.query.DeviceTypeFilter;
import com.jnks.iot.server.edqs.query.EdqsQuery;
import com.jnks.iot.server.edqs.repo.TenantRepo;

import java.util.List;

public class DeviceTypeQueryProcessor extends AbstractEntityProfileQueryProcessor<DeviceTypeFilter> {

    public DeviceTypeQueryProcessor(TenantRepo repo, QueryContext ctx, EdqsQuery query) {
        super(repo, ctx, query, (DeviceTypeFilter) query.getEntityFilter(), EntityType.DEVICE);
    }

    @Override
    protected String getEntityNameFilter(DeviceTypeFilter filter) {
        return filter.getDeviceNameFilter();
    }

    @Override
    protected List<String> getProfileNames(DeviceTypeFilter filter) {
        return filter.getDeviceTypes();
    }

    @Override
    protected EntityType getProfileEntityType() {
        return EntityType.DEVICE_PROFILE;
    }

}
