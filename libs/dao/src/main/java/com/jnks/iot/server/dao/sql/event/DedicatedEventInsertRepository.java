package com.jnks.iot.server.dao.sql.event;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;
import com.jnks.iot.server.dao.config.DedicatedEventsDataSource;

import static com.jnks.iot.server.dao.config.DedicatedEventsJpaDaoConfig.EVENTS_JDBC_TEMPLATE;
import static com.jnks.iot.server.dao.config.DedicatedEventsJpaDaoConfig.EVENTS_TRANSACTION_TEMPLATE;

@DedicatedEventsDataSource
@Repository
public class DedicatedEventInsertRepository extends EventInsertRepository {

    public DedicatedEventInsertRepository(@Qualifier(EVENTS_JDBC_TEMPLATE) JdbcTemplate jdbcTemplate,
                                          @Qualifier(EVENTS_TRANSACTION_TEMPLATE) TransactionTemplate transactionTemplate) {
        super(jdbcTemplate, transactionTemplate);
    }

}
