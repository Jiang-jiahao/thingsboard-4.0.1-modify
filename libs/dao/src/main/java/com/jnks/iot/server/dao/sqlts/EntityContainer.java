package com.jnks.iot.server.dao.sqlts;

import lombok.AllArgsConstructor;
import lombok.Data;
import com.jnks.iot.server.dao.model.sql.AbstractTsKvEntity;

@Data
@AllArgsConstructor
public class EntityContainer<T extends AbstractTsKvEntity> {

        private T entity;
        private String partitionDate;

}
