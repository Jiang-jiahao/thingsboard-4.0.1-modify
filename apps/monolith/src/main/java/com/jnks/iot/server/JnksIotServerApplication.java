package com.jnks.iot.server;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.Ordered;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import com.jnks.iot.common.util.AfterStartUp;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@SpringBootConfiguration
@EnableAsync
@EnableScheduling
@ComponentScan(
        basePackages = {"com.jnks.iot.server", "com.jnks.iot.script", "com.jnks.iot.monolith", "com.jnks.iot.edqs"},
        excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = {
                JnksIotCoreServerApplication.class,
                JnksIotRuleEngineServerApplication.class
        })
)
@Slf4j
public class JnksIotServerApplication {

    private static final String SPRING_CONFIG_NAME_KEY = "--spring.config.name";
    private static final String DEFAULT_SPRING_CONFIG_PARAM = SPRING_CONFIG_NAME_KEY + "=" + "jnks-iot";

    private static long startTs;

    public static void main(String[] args) {
        startTs = System.currentTimeMillis();
        SpringApplication.run(JnksIotServerApplication.class, updateArguments(args));
    }

    private static String[] updateArguments(String[] args) {
        if (Arrays.stream(args).noneMatch(arg -> arg.startsWith(SPRING_CONFIG_NAME_KEY))) {
            String[] modifiedArgs = new String[args.length + 1];
            System.arraycopy(args, 0, modifiedArgs, 0, args.length);
            modifiedArgs[args.length] = DEFAULT_SPRING_CONFIG_PARAM;
            return modifiedArgs;
        }
        return args;
    }

    @AfterStartUp(order = Ordered.LOWEST_PRECEDENCE)
    public void afterStartUp() {
        long startupTimeMs = System.currentTimeMillis() - startTs;
        log.info("Started JnksIOT in {} seconds", TimeUnit.MILLISECONDS.toSeconds(startupTimeMs));
    }

}
