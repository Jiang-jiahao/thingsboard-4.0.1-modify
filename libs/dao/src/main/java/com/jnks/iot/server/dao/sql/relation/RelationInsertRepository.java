package com.jnks.iot.server.dao.sql.relation;

import com.jnks.iot.server.dao.model.sql.RelationEntity;

import java.util.List;

public interface RelationInsertRepository {

    RelationEntity saveOrUpdate(RelationEntity entity);

    List<RelationEntity> saveOrUpdate(List<RelationEntity> entities);

}