package com.jnks.iot.server.dao.service.event.sql;

import org.springframework.test.context.TestPropertySource;
import com.jnks.iot.server.dao.service.DaoSqlTest;

@DaoSqlTest
@TestPropertySource(properties = {
        "spring.datasource.events.enabled=true",
        "spring.datasource.events.url=${spring.datasource.url}",
        "spring.datasource.events.driverClassName=${spring.datasource.driverClassName}"
})
public class EventServiceSqlTest_DedicatedEventsDataSource extends EventServiceSqlTest {
}
