package org.thingsboard.server.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import org.thingsboard.server.actors.tenant.TenantDeviceActorSupport;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity // 这个注解暂时没什么作用
@Order(SecurityProperties.BASIC_AUTH_ORDER)
@ConditionalOnMissingBean(TenantDeviceActorSupport.class)
public class TbRuleEngineSecurityConfiguration {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.headers(headers -> headers
                        .cacheControl(config -> {})
                        .frameOptions(config -> {}).disable())
                .cors(cors -> {})
                .csrf(AbstractHttpConfigurer::disable)
                .authorizeHttpRequests(config -> config
                        .requestMatchers("/actuator/prometheus").permitAll()
                        .anyRequest().authenticated());
        return http.build();
    }
}
