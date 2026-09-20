package com.jnks.iot.server.dao.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.repository.config.BootstrapMode;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import com.jnks.iot.server.dao.util.SqlTsLatestDao;
import com.jnks.iot.server.dao.util.TbAutoConfiguration;

@Configuration
@TbAutoConfiguration
@ComponentScan({"com.jnks.iot.server.dao.sqlts.sql"})
@EnableJpaRepositories(value = {"com.jnks.iot.server.dao.sqlts.insert.latest.sql", "com.jnks.iot.server.dao.sqlts.latest"}, bootstrapMode = BootstrapMode.LAZY)
@EnableTransactionManagement
@SqlTsLatestDao
public class SqlTsLatestDaoConfig {

}
