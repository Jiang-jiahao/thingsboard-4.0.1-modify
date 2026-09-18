package org.thingsboard.server.dao;

import org.thingsboard.server.common.data.id.HasId;
import org.thingsboard.server.common.data.id.TenantId;

import java.util.List;

public interface ResourceContainerDao<T extends HasId<?>> {

    List<T> findByTenantIdAndResourceLink(TenantId tenantId, String link, int limit);

    List<T> findByResourceLink(String link, int limit);

}
