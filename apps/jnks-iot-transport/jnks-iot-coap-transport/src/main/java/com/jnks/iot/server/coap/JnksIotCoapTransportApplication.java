package com.jnks.iot.server.coap;

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
@ComponentScan({"com.jnks.iot.server.coap", "com.jnks.iot.server.common", "com.jnks.iot.server.coapserver", "com.jnks.iot.server.transport.coap", "com.jnks.iot.server.queue", "com.jnks.iot.server.cache", "com.jnks.iot.transport.base", "com.jnks.iot.transport.provider"})
public class JnksIotCoapTransportApplication {

    private static final String SPRING_CONFIG_NAME_KEY = "--spring.config.name";
    private static final String DEFAULT_SPRING_CONFIG_PARAM = SPRING_CONFIG_NAME_KEY + "=" + "jnks-iot-coap-transport";

    public static void main(String[] args) {
        SpringApplication.run(JnksIotCoapTransportApplication.class, updateArguments(args));
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
