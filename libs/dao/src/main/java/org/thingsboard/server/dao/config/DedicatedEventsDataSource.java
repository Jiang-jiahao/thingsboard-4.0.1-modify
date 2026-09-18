package org.thingsboard.server.dao.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@ConditionalOnProperty(value = "spring.datasource.events.enabled", havingValue = "true")
public @interface DedicatedEventsDataSource {
}
