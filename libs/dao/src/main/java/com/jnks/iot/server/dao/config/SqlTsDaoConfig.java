package com.jnks.iot.server.dao.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.repository.config.BootstrapMode;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import com.jnks.iot.server.dao.util.SqlTsDao;
import com.jnks.iot.server.dao.util.JnksIotAutoConfiguration;

@Configuration
@JnksIotAutoConfiguration
@ComponentScan({"com.jnks.iot.server.dao.sqlts.sql", "com.jnks.iot.server.dao.sqlts.insert.sql"})
@EnableJpaRepositories(value = {"com.jnks.iot.server.dao.sqlts.ts", "com.jnks.iot.server.dao.sqlts.insert.sql"}, bootstrapMode = BootstrapMode.LAZY)
@EnableTransactionManagement
@SqlTsDao
public class SqlTsDaoConfig {

}
