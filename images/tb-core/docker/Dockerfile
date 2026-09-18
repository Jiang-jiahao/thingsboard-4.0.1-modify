#
# Copyright © 2016-2025 The Thingsboard Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM thingsboard/openjdk17:bookworm-slim

# 直接使用 Maven 产出的 tb-core boot jar（不再装 deb）；配置由 compose 挂载到 /config。
COPY start-tb-core.sh ${pkg.name}.jar /tmp/

# data 目录即 install.data_dir 的落点：InstallScripts 在启动时会校验它必须存在
COPY data ${pkg.installFolder}/data

RUN chmod a+x /tmp/*.sh \
    && mv /tmp/start-tb-core.sh /usr/bin \
    && (id -u ${pkg.user} > /dev/null 2>&1 || useradd --system --create-home ${pkg.user}) \
    && mkdir -p ${pkg.installFolder}/bin ${pkg.unixLogFolder} \
    && mv /tmp/${pkg.name}.jar ${pkg.installFolder}/bin/${pkg.name}.jar \
    && chmod 555 ${pkg.installFolder}/bin/${pkg.name}.jar \
    && chown -R ${pkg.user}:${pkg.user} ${pkg.installFolder} ${pkg.unixLogFolder}

USER ${pkg.user}

CMD ["start-tb-core.sh"]
