package org.thingsboard.server.dao.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.repository.config.BootstrapMode;

@DefaultDataSource
@Configuration
@EnableJpaRepositories(value = {"org.thingsboard.server.dao.sql.event", "org.thingsboard.server.dao.sql.audit"}, bootstrapMode = BootstrapMode.LAZY)
public class DefaultDedicatedJpaDaoConfig {

}
