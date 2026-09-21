package com.jnks.iot.server.edqs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Arrays;

@SpringBootConfiguration
@EnableAsync
@EnableScheduling
@EnableAutoConfiguration
@ComponentScan({"com.jnks.iot.server.edqs", "com.jnks.iot.server.queue.edqs", "com.jnks.iot.edqs", "com.jnks.iot.server.queue.discovery", "com.jnks.iot.server.queue.kafka",
        "com.jnks.iot.server.queue.environment", "com.jnks.iot.server.common.stats"})
@Slf4j
public class JnksIotEdqsApplication {

    private static final String SPRING_CONFIG_NAME_KEY = "--spring.config.name";
    private static final String DEFAULT_SPRING_CONFIG_PARAM = SPRING_CONFIG_NAME_KEY + "=" + "jnks-iot-edqs";

    public static void main(String[] args) {
        SpringApplication.run(JnksIotEdqsApplication.class, updateArguments(args));
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

}
