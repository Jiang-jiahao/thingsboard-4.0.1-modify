package com.jnks.iot.server.dao.service;

import com.jnks.iot.server.common.data.id.IdBased;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.TimePageLink;

public abstract class TimePaginatedRemover<I, D extends IdBased<?>> {

    private static final int DEFAULT_LIMIT = 100;

    public void removeEntities(TenantId tenantId, I id) {
        TimePageLink pageLink = new TimePageLink(DEFAULT_LIMIT);
        boolean hasNext = true;
        while (hasNext) {
            PageData<D> entities = findEntities(tenantId, id, pageLink);
            for (D entity : entities.getData()) {
                removeEntity(tenantId, entity);
            }
            hasNext = entities.hasNext();
        }
    }

    protected abstract PageData<D> findEntities(TenantId tenantId, I id, TimePageLink pageLink);

    protected abstract void removeEntity(TenantId tenantId, D entity);

}
