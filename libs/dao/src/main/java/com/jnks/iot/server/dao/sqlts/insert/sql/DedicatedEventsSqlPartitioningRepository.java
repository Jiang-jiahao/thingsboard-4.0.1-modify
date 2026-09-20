package com.jnks.iot.server.dao.sqlts.insert.sql;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.jnks.iot.server.dao.config.DedicatedEventsDataSource;
import com.jnks.iot.server.dao.timeseries.SqlPartition;

import static com.jnks.iot.server.dao.config.DedicatedEventsJpaDaoConfig.EVENTS_JDBC_TEMPLATE;
import static com.jnks.iot.server.dao.config.DedicatedEventsJpaDaoConfig.EVENTS_TRANSACTION_MANAGER;

@DedicatedEventsDataSource
@Repository
public class DedicatedEventsSqlPartitioningRepository extends SqlPartitioningRepository {

    @Autowired
    @Qualifier(EVENTS_JDBC_TEMPLATE)
    private JdbcTemplate jdbcTemplate;

    @Transactional(propagation = Propagation.NOT_SUPPORTED, transactionManager = EVENTS_TRANSACTION_MANAGER)
    @Override
    public void save(SqlPartition partition) {
        super.save(partition);
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, transactionManager = EVENTS_TRANSACTION_MANAGER)
    @Override
    public void createPartitionIfNotExists(String table, long entityTs, long partitionDurationMs) {
        super.createPartitionIfNotExists(table, entityTs, partitionDurationMs);
    }

    @Override
    protected JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

}
