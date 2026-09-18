FROM thingsboard/openjdk17:bookworm-slim

# 直接使用 Maven 产出的 tb-rule-engine boot jar（不再装 deb）；配置由 compose 挂载到 /config。
COPY start-tb-rule-engine.sh ${pkg.name}.jar /tmp/

# data 目录即 install.data_dir 的落点：InstallScripts 在启动时会校验它必须存在
COPY data ${pkg.installFolder}/data

RUN chmod a+x /tmp/*.sh \
    && mv /tmp/start-tb-rule-engine.sh /usr/bin \
    && (id -u ${pkg.user} > /dev/null 2>&1 || useradd --system --create-home ${pkg.user}) \
    && mkdir -p ${pkg.installFolder}/bin ${pkg.unixLogFolder} \
    && mv /tmp/${pkg.name}.jar ${pkg.installFolder}/bin/${pkg.name}.jar \
    && chmod 555 ${pkg.installFolder}/bin/${pkg.name}.jar \
    && chown -R ${pkg.user}:${pkg.user} ${pkg.installFolder} ${pkg.unixLogFolder}

USER ${pkg.user}

CMD ["start-tb-rule-engine.sh"]
