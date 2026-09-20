package com.jnks.iot.server.dao.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.repository.config.BootstrapMode;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import com.jnks.iot.server.dao.util.TbAutoConfiguration;
import com.jnks.iot.server.dao.util.TimescaleDBTsLatestDao;

@Configuration
@TbAutoConfiguration
@ComponentScan({"com.jnks.iot.server.dao.sqlts.timescale"})
@EnableJpaRepositories(value = {"com.jnks.iot.server.dao.sqlts.insert.latest.sql", "com.jnks.iot.server.dao.sqlts.latest"}, bootstrapMode = BootstrapMode.LAZY)
@EnableTransactionManagement
@TimescaleDBTsLatestDao
public class TimescaleTsLatestDaoConfig {

}
