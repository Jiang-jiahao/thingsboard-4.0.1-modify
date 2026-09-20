package com.jnks.iot.server.dao.sql;

import com.jnks.iot.server.dao.model.BaseEntity;
import com.jnks.iot.server.dao.util.SqlDao;

@SqlDao
public abstract class JpaPartitionedAbstractDao<E extends BaseEntity<D>, D> extends JpaAbstractDao<E, D> {

    @Override
    protected E doSave(E entity, boolean isNew, boolean flush) {
        createPartition(entity);
        return super.doSave(entity, isNew, flush);
    }

    public abstract void createPartition(E entity);

}
