package com.jnks.iot.server.dao.sqlts.insert.latest;

import com.jnks.iot.server.dao.model.sqlts.latest.TsKvLatestEntity;

import java.util.List;

public interface InsertLatestTsRepository {

    List<Long> saveOrUpdate(List<TsKvLatestEntity> entities);

}
