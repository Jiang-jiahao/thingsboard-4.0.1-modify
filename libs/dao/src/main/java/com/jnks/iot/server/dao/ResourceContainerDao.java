package com.jnks.iot.server.dao;

import com.jnks.iot.server.common.data.id.HasId;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.List;

public interface ResourceContainerDao<T extends HasId<?>> {

    List<T> findByTenantIdAndResourceLink(TenantId tenantId, String link, int limit);

    List<T> findByResourceLink(String link, int limit);

}
