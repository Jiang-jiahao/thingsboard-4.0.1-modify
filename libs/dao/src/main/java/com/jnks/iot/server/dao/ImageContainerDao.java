package com.jnks.iot.server.dao;

import com.jnks.iot.server.common.data.id.HasId;
import com.jnks.iot.server.common.data.id.TenantId;

import java.util.List;

public interface ImageContainerDao<T extends HasId<?>> {

    List<T> findByTenantAndImageLink(TenantId tenantId, String imageUrl, int limit);

    List<T> findByImageLink(String imageUrl, int limit);

}
