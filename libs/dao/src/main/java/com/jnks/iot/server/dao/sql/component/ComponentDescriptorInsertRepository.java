package com.jnks.iot.server.dao.sql.component;

import com.jnks.iot.server.dao.model.sql.ComponentDescriptorEntity;

public interface ComponentDescriptorInsertRepository {

    ComponentDescriptorEntity saveOrUpdate(ComponentDescriptorEntity entity);

}
