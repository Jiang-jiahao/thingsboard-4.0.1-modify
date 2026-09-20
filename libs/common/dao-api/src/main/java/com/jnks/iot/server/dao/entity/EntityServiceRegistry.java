package com.jnks.iot.server.dao.entity;

import com.jnks.iot.server.common.data.EntityType;

public interface EntityServiceRegistry {

    EntityDaoService getServiceByEntityType(EntityType entityType);

}
