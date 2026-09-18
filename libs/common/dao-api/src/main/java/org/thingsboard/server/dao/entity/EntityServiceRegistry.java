package org.thingsboard.server.dao.entity;

import org.thingsboard.server.common.data.EntityType;

public interface EntityServiceRegistry {

    EntityDaoService getServiceByEntityType(EntityType entityType);

}
