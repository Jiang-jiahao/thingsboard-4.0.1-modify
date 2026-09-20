package com.jnks.iot.server.dao.sqlts.insert;

import com.jnks.iot.server.dao.model.sql.AbstractTsKvEntity;

import java.util.List;

public interface InsertTsRepository<T extends AbstractTsKvEntity> {

    void saveOrUpdate(List<T> entities);

}
