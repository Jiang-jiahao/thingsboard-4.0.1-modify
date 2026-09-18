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

# 直接使用 Maven 产出的 boot jar（不再装 deb）。/config 由 compose 挂载提供，
# 启动脚本会 source /config/${pkg.name}.conf 并 -Dlogging.config=/config/logback.xml。
COPY start-tb-mqtt-transport.sh ${pkg.name}.jar /tmp/

RUN chmod a+x /tmp/*.sh \
    && mv /tmp/start-tb-mqtt-transport.sh /usr/bin \
    && (id -u ${pkg.user} > /dev/null 2>&1 || useradd --system --create-home ${pkg.user}) \
    && mkdir -p ${pkg.installFolder}/bin ${pkg.logFolder} \
    && mv /tmp/${pkg.name}.jar ${pkg.installFolder}/bin/${pkg.name}.jar \
    && chmod 555 ${pkg.installFolder}/bin/${pkg.name}.jar \
    && chown -R ${pkg.user}:${pkg.user} ${pkg.installFolder} ${pkg.logFolder}

USER ${pkg.user}

CMD ["start-tb-mqtt-transport.sh"]
