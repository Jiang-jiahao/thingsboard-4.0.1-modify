package com.jnks.iot.server.dao;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;

public interface TenantEntityDao<T> {

    default Long countByTenantId(TenantId tenantId) {
        throw new UnsupportedOperationException();
    }

    default PageData<T> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        throw new UnsupportedOperationException();
    }

}
