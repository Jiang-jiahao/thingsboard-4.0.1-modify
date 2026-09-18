package org.thingsboard.server.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

@Configuration
public class CryptoConfig {

    /**
     * 创建 BCrypt 强哈希算法，用于实现密码的安全存储
     * @return BCryptPasswordEncoder
     */
    @Bean
    protected BCryptPasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

}
