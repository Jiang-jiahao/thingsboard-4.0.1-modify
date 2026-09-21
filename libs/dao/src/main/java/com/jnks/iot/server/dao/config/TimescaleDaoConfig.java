package com.jnks.iot.server.dao.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.repository.config.BootstrapMode;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import com.jnks.iot.server.dao.util.JnksIotAutoConfiguration;
import com.jnks.iot.server.dao.util.TimescaleDBTsDao;

@Configuration
@JnksIotAutoConfiguration
@ComponentScan({"com.jnks.iot.server.dao.sqlts.timescale"})
@EnableJpaRepositories(value = {"com.jnks.iot.server.dao.sqlts.timescale", "com.jnks.iot.server.dao.sqlts.insert.timescale"}, bootstrapMode = BootstrapMode.LAZY)
@EnableTransactionManagement
@TimescaleDBTsDao
public class TimescaleDaoConfig {

}
