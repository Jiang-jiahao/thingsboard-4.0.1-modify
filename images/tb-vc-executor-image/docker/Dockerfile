FROM thingsboard/openjdk17:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -yq --no-install-recommends ncdu \
    && rm -rf /var/lib/apt/lists/*

# 直接使用 Maven 产出的 boot jar（不再装 deb）；配置目录由 compose 挂载提供。
COPY start-tb-vc-executor.sh ${pkg.name}.jar /tmp/

RUN mkdir -p /home/thingsboard/.config/jgit \
    && chown -R ${pkg.user}:${pkg.user} /home/thingsboard \
    && chmod a+x /tmp/*.sh \
    && mv /tmp/start-tb-vc-executor.sh /usr/bin \
    && (id -u ${pkg.user} > /dev/null 2>&1 || useradd --system --create-home ${pkg.user}) \
    && mkdir -p ${pkg.installFolder}/bin ${pkg.logFolder} \
    && mv /tmp/${pkg.name}.jar ${pkg.installFolder}/bin/${pkg.name}.jar \
    && chmod 555 ${pkg.installFolder}/bin/${pkg.name}.jar \
    && chown -R ${pkg.user}:${pkg.user} ${pkg.installFolder} ${pkg.logFolder}

USER ${pkg.user}

CMD ["start-tb-vc-executor.sh"]
