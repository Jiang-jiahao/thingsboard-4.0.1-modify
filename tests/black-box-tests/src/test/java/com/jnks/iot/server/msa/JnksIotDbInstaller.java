package com.jnks.iot.server.msa;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.utility.Base58;
import com.jnks.iot.server.common.data.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class JnksIotDbInstaller {

    final static boolean IS_REDIS_CLUSTER = Boolean.parseBoolean(System.getProperty("blackBoxTests.redisCluster"));
    final static boolean IS_REDIS_SENTINEL = Boolean.parseBoolean(System.getProperty("blackBoxTests.redisSentinel"));
    final static boolean IS_HYBRID_MODE = Boolean.parseBoolean(System.getProperty("blackBoxTests.hybridMode"));
    private final static String POSTGRES_DATA_VOLUME = "jnks-iot-postgres-test-data-volume";

    private final static String CASSANDRA_DATA_VOLUME = "jnks-iot-cassandra-test-data-volume";
    private final static String REDIS_DATA_VOLUME = "jnks-iot-redis-data-volume";
    private final static String REDIS_CLUSTER_DATA_VOLUME = "jnks-iot-redis-cluster-data-volume";
    private final static String REDIS_SENTINEL_DATA_VOLUME = "jnks-iot-redis-sentinel-data-volume";
    private final static String JNKS_IOT_LOG_VOLUME = "jnks-iot-log-test-volume";
    private final static String JNKS_IOT_COAP_TRANSPORT_LOG_VOLUME = "jnks-iot-coap-transport-log-test-volume";
    private final static String JNKS_IOT_LWM2M_TRANSPORT_LOG_VOLUME = "jnks-iot-lwm2m-transport-log-test-volume";
    private final static String JNKS_IOT_HTTP_TRANSPORT_LOG_VOLUME = "jnks-iot-http-transport-log-test-volume";
    private final static String JNKS_IOT_MQTT_TRANSPORT_LOG_VOLUME = "jnks-iot-mqtt-transport-log-test-volume";
    private final static String JNKS_IOT_SNMP_TRANSPORT_LOG_VOLUME = "jnks-iot-snmp-transport-log-test-volume";
    private final static String JNKS_IOT_VC_EXECUTOR_LOG_VOLUME = "jnks-iot-vc-executor-log-test-volume";
    private final static String JNKS_IOT_EDQS_LOG_VOLUME = "jnks-iot-edqs-log-test-volume";
    private final static String JAVA_OPTS = "-Xmx512m";

    private final DockerComposeExecutor dockerCompose;

    private final String postgresDataVolume;
    private final String cassandraDataVolume;

    private final String redisDataVolume;
    private final String redisClusterDataVolume;
    private final String redisSentinelDataVolume;
    private final String jnksIotLogVolume;
    private final String jnksIotCoapTransportLogVolume;
    private final String jnksIotLwm2mTransportLogVolume;
    private final String jnksIotHttpTransportLogVolume;
    private final String jnksIotMqttTransportLogVolume;
    private final String jnksIotSnmpTransportLogVolume;
    private final String jnksIotVcExecutorLogVolume;
    private final String jnksIotEdqsLogVolume;
    private final Map<String, String> env;

    public JnksIotDbInstaller() {
        log.info("System property of blackBoxTests.redisCluster is {}", IS_REDIS_CLUSTER);
        log.info("System property of blackBoxTests.redisCluster is {}", IS_REDIS_SENTINEL);
        log.info("System property of blackBoxTests.hybridMode is {}", IS_HYBRID_MODE);
        List<File> composeFiles = new ArrayList<>(Arrays.asList(
                new File("./../../docker/docker-compose.yml"),
                new File("./../../docker/docker-compose.volumes.yml"),
                IS_HYBRID_MODE
                        ? new File("./../../docker/docker-compose.hybrid.yml")
                        : new File("./../../docker/docker-compose.postgres.yml"),
                new File("./../../docker/docker-compose.postgres.volumes.yml"),
                resolveRedisComposeFile(),
                resolveRedisComposeVolumesFile()
        ));
        if (IS_HYBRID_MODE) {
            composeFiles.add(new File("./../../docker/docker-compose.cassandra.volumes.yml"));
            composeFiles.add(new File("src/test/resources/docker-compose.hybrid-test-extras.yml"));
        } else {
            composeFiles.add(new File("src/test/resources/docker-compose.postgres-test-extras.yml"));
        }

        String identifier = Base58.randomString(6).toLowerCase();
        String project = identifier + Base58.randomString(6).toLowerCase();

        postgresDataVolume = project + "_" + POSTGRES_DATA_VOLUME;
        cassandraDataVolume = project + "_" + CASSANDRA_DATA_VOLUME;
        redisDataVolume = project + "_" + REDIS_DATA_VOLUME;
        redisClusterDataVolume = project + "_" + REDIS_CLUSTER_DATA_VOLUME;
        redisSentinelDataVolume = project + "_" + REDIS_SENTINEL_DATA_VOLUME;
        jnksIotLogVolume = project + "_" + JNKS_IOT_LOG_VOLUME;
        jnksIotCoapTransportLogVolume = project + "_" + JNKS_IOT_COAP_TRANSPORT_LOG_VOLUME;
        jnksIotLwm2mTransportLogVolume = project + "_" + JNKS_IOT_LWM2M_TRANSPORT_LOG_VOLUME;
        jnksIotHttpTransportLogVolume = project + "_" + JNKS_IOT_HTTP_TRANSPORT_LOG_VOLUME;
        jnksIotMqttTransportLogVolume = project + "_" + JNKS_IOT_MQTT_TRANSPORT_LOG_VOLUME;
        jnksIotSnmpTransportLogVolume = project + "_" + JNKS_IOT_SNMP_TRANSPORT_LOG_VOLUME;
        jnksIotVcExecutorLogVolume = project + "_" + JNKS_IOT_VC_EXECUTOR_LOG_VOLUME;
        jnksIotEdqsLogVolume = project + "_" + JNKS_IOT_EDQS_LOG_VOLUME;

        dockerCompose = new DockerComposeExecutor(composeFiles, project);

        env = new HashMap<>();
        env.put("JAVA_OPTS", JAVA_OPTS);
        env.put("POSTGRES_DATA_VOLUME", postgresDataVolume);
        if (IS_HYBRID_MODE) {
            env.put("CASSANDRA_DATA_VOLUME", cassandraDataVolume);
        }
        env.put("JNKS_IOT_LOG_VOLUME", jnksIotLogVolume);
        env.put("JNKS_IOT_COAP_TRANSPORT_LOG_VOLUME", jnksIotCoapTransportLogVolume);
        env.put("JNKS_IOT_LWM2M_TRANSPORT_LOG_VOLUME", jnksIotLwm2mTransportLogVolume);
        env.put("JNKS_IOT_HTTP_TRANSPORT_LOG_VOLUME", jnksIotHttpTransportLogVolume);
        env.put("JNKS_IOT_MQTT_TRANSPORT_LOG_VOLUME", jnksIotMqttTransportLogVolume);
        env.put("JNKS_IOT_SNMP_TRANSPORT_LOG_VOLUME", jnksIotSnmpTransportLogVolume);
        env.put("JNKS_IOT_VC_EXECUTOR_LOG_VOLUME", jnksIotVcExecutorLogVolume);
        env.put("JNKS_IOT_EDQS_LOG_VOLUME", jnksIotEdqsLogVolume);
        if (IS_REDIS_CLUSTER) {
            for (int i = 0; i < 6; i++) {
                env.put("REDIS_CLUSTER_DATA_VOLUME_" + i, redisClusterDataVolume + '-' + i);
            }
        } else if (IS_REDIS_SENTINEL) {
            env.put("REDIS_SENTINEL_DATA_VOLUME_MASTER", redisSentinelDataVolume + "-" + "master");
            env.put("REDIS_SENTINEL_DATA_VOLUME_SLAVE", redisSentinelDataVolume + "-" + "slave");
            env.put("REDIS_SENTINEL_DATA_VOLUME_SENTINEL", redisSentinelDataVolume + "-" + "sentinel");
        } else {
            env.put("REDIS_DATA_VOLUME", redisDataVolume);
        }
        dockerCompose.withEnv(env);
    }

    private static File resolveRedisComposeVolumesFile() {
        if (IS_REDIS_CLUSTER) {
            return new File("./../../docker/docker-compose.redis-cluster.volumes.yml");
        }
        if (IS_REDIS_SENTINEL) {
            return new File("./../../docker/docker-compose.redis-sentinel.volumes.yml");
        }
        return new File("./../../docker/docker-compose.redis.volumes.yml");
    }

    private static File resolveRedisComposeFile() {
        if (IS_REDIS_CLUSTER) {
            return new File("./../../docker/docker-compose.redis-cluster.yml");
        }
        if (IS_REDIS_SENTINEL) {
            return new File("./../../docker/docker-compose.redis-sentinel.yml");
        }
        return new File("./../../docker/docker-compose.redis.yml");
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public void createVolumes() {
        try {

            dockerCompose.withCommand("volume create " + postgresDataVolume);
            dockerCompose.invokeDocker();

            if (IS_HYBRID_MODE) {
                dockerCompose.withCommand("volume create " + cassandraDataVolume);
                dockerCompose.invokeDocker();
            }

            dockerCompose.withCommand("volume create " + jnksIotLogVolume);
            dockerCompose.invokeDocker();

            dockerCompose.withCommand("volume create " + jnksIotCoapTransportLogVolume);
            dockerCompose.invokeDocker();

            dockerCompose.withCommand("volume create " + jnksIotLwm2mTransportLogVolume);
            dockerCompose.invokeDocker();

            dockerCompose.withCommand("volume create " + jnksIotHttpTransportLogVolume);
            dockerCompose.invokeDocker();

            dockerCompose.withCommand("volume create " + jnksIotMqttTransportLogVolume);
            dockerCompose.invokeDocker();

            dockerCompose.withCommand("volume create " + jnksIotSnmpTransportLogVolume);
            dockerCompose.invokeDocker();

            dockerCompose.withCommand("volume create " + jnksIotVcExecutorLogVolume);
            dockerCompose.invokeDocker();

            dockerCompose.withCommand("volume create " + jnksIotEdqsLogVolume);
            dockerCompose.invokeDocker();

            StringBuilder additionalServices = new StringBuilder();
            if (IS_HYBRID_MODE) {
                additionalServices.append(" cassandra");
            }
            if (IS_REDIS_CLUSTER) {
                for (int i = 0; i < 6; i++) {
                    additionalServices.append(" redis-node-").append(i);
                    dockerCompose.withCommand("volume create " + redisClusterDataVolume + '-' + i);
                    dockerCompose.invokeDocker();
                }
            } else if (IS_REDIS_SENTINEL) {
                additionalServices.append(" redis-master");
                dockerCompose.withCommand("volume create " + redisSentinelDataVolume + "-" + "master");
                dockerCompose.invokeDocker();

                additionalServices.append(" redis-slave");
                dockerCompose.withCommand("volume create " + redisSentinelDataVolume + '-' + "slave");
                dockerCompose.invokeDocker();

                additionalServices.append(" redis-sentinel");
                dockerCompose.withCommand("volume create " + redisSentinelDataVolume + '-' + "sentinel");
                dockerCompose.invokeDocker();
            } else {
                additionalServices.append(" redis");
                dockerCompose.withCommand("volume create " + redisDataVolume);
                dockerCompose.invokeDocker();
            }

            dockerCompose.withCommand("up -d postgres" + additionalServices);
            dockerCompose.invokeCompose();

            dockerCompose.withCommand("run --no-deps --rm -e INSTALL_TB=true -e LOAD_DEMO=true " +
                    "jnks-iot-core1");
            dockerCompose.invokeCompose();

        } finally {
            try {
                dockerCompose.withCommand("down -v");
                dockerCompose.invokeCompose();
            } catch (Exception ignored) {
            }
        }
    }

    public void savaLogsAndRemoveVolumes() {
        copyLogs(jnksIotLogVolume, "./target/jnks-iot-logs/");
        copyLogs(jnksIotCoapTransportLogVolume, "./target/jnks-iot-coap-transport-logs/");
        copyLogs(jnksIotLwm2mTransportLogVolume, "./target/jnks-iot-lwm2m-transport-logs/");
        copyLogs(jnksIotHttpTransportLogVolume, "./target/jnks-iot-http-transport-logs/");
        copyLogs(jnksIotMqttTransportLogVolume, "./target/jnks-iot-mqtt-transport-logs/");
        copyLogs(jnksIotSnmpTransportLogVolume, "./target/jnks-iot-snmp-transport-logs/");
        copyLogs(jnksIotVcExecutorLogVolume, "./target/jnks-iot-vc-executor-logs/");
        copyLogs(jnksIotEdqsLogVolume, "./target/jnks-iot-edqs-logs/");

        StringJoiner rmVolumesCommand = new StringJoiner(" ")
                .add("volume rm -f")
                .add(postgresDataVolume)
                .add(jnksIotLogVolume)
                .add(jnksIotCoapTransportLogVolume)
                .add(jnksIotLwm2mTransportLogVolume)
                .add(jnksIotHttpTransportLogVolume)
                .add(jnksIotMqttTransportLogVolume)
                .add(jnksIotSnmpTransportLogVolume)
                .add(jnksIotVcExecutorLogVolume)
                .add(jnksIotEdqsLogVolume)
                .add(resolveRedisComposeVolumeLog());

        if (IS_HYBRID_MODE) {
            rmVolumesCommand.add(cassandraDataVolume);
        }

        dockerCompose.withCommand(rmVolumesCommand.toString());
    }

    private String resolveRedisComposeVolumeLog() {
        if (IS_REDIS_CLUSTER) {
            return IntStream.range(0, 6).mapToObj(i -> " " + redisClusterDataVolume + "-" + i).collect(Collectors.joining());
        }
        if (IS_REDIS_SENTINEL) {
            return redisSentinelDataVolume + "-" + "master " + " " +
                    redisSentinelDataVolume + "-" + "slave" + " " +
                    redisSentinelDataVolume + " " + "sentinel";
        }
        return redisDataVolume;
    }

    private void copyLogs(String volumeName, String targetDir) {
        File jnksIotLogsDir = new File(targetDir);
        jnksIotLogsDir.mkdirs();

        String logsContainerName = "jnks-iot-logs-container-" + StringUtils.randomAlphanumeric(10);

        dockerCompose.withCommand("run -d --rm --name " + logsContainerName + " -v " + volumeName + ":/root alpine tail -f /dev/null");
        dockerCompose.invokeDocker();

        dockerCompose.withCommand("cp " + logsContainerName + ":/root/. " + jnksIotLogsDir.getAbsolutePath());
        dockerCompose.invokeDocker();

        dockerCompose.withCommand("rm -f " + logsContainerName);
        dockerCompose.invokeDocker();
    }

}
