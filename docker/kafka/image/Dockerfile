# 自建 Kafka 镜像：标准 Apache Kafka 发行版 + JRE，用本机已有的发行版打包，**构建不需要联网**。
#
# 为什么要自建：本环境（以及目标内网）的镜像加速站拉不到任何 Kafka 镜像 ——
# bitnami/kafka 直接 denied；apache/kafka 0 字节；confluentinc/cp-kafka 卡在 10~11/13 层；
# bitnamilegacy/kafka 也是 0 字节。而 Redis（redis:7-alpine）能拉，所以只有 Kafka 需要这样处理。
#
# 镜像里跑的是**标准 Apache Kafka**（发行版原样 COPY 进去），只是打包方式不是 Bitnami/Confluent 的。
# 如果哪天网络能拉官方镜像了，把 docker/deploy.sh 里对应的构建步骤去掉、再把
# docker-compose.kafka.yml 的 image 换回 confluentinc/cp-kafka:7.7.0 即可 —— kafka.env 里的
# 变量名（标准 KAFKA_* + KRaft）和它是兼容的，不用改。
FROM eclipse-temurin:17-jre-noble

ENV KAFKA_HOME=/opt/kafka

COPY kafka ${KAFKA_HOME}
COPY docker-entrypoint.sh /usr/local/bin/kafka-entrypoint.sh

RUN chmod +x /usr/local/bin/kafka-entrypoint.sh \
    && mkdir -p /var/lib/kafka/data /var/lib/kafka/logs \
    && useradd --system --uid 999 --create-home kafka \
    && chown -R kafka:kafka ${KAFKA_HOME} /var/lib/kafka

ENV LOG_DIR=/var/lib/kafka/logs
USER kafka

EXPOSE 9092 9094
ENTRYPOINT ["kafka-entrypoint.sh"]
