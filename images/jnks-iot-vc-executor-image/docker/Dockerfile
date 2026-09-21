FROM thingsboard/openjdk17:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -yq --no-install-recommends ncdu \
    && rm -rf /var/lib/apt/lists/*

# 直接使用 Maven 产出的 boot jar（不再装 deb）；配置目录由 compose 挂载提供。
COPY start-tb-vc-executor.sh ${pkg.name}.jar /tmp/

# 先建运行用户再 chown：基础镜像 thingsboard/openjdk17 自带的是 thingsboard 用户，
# 换成我们自己的用户名后必须自己创建，否则 chown 会因用户不存在而失败。
RUN (id -u ${pkg.user} > /dev/null 2>&1 || useradd --system --create-home ${pkg.user}) \
    && mkdir -p /home/${pkg.user}/.config/jgit \
    && chown -R ${pkg.user}:${pkg.user} /home/${pkg.user} \
    && chmod a+x /tmp/*.sh \
    && mv /tmp/start-tb-vc-executor.sh /usr/bin \
    && mkdir -p ${pkg.installFolder}/bin ${pkg.logFolder} \
    && mv /tmp/${pkg.name}.jar ${pkg.installFolder}/bin/${pkg.name}.jar \
    && chmod 555 ${pkg.installFolder}/bin/${pkg.name}.jar \
    && chown -R ${pkg.user}:${pkg.user} ${pkg.installFolder} ${pkg.logFolder}

USER ${pkg.user}

CMD ["start-tb-vc-executor.sh"]
