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

# Web UI 静态托管：由 nginx 直接发文件（不再需要 Node 运行时）。
#
# 上游的 web-ui 是个 Express 服务，但它实际只做静态托管 + SPA 回退 + gzip + （可选的）API 转发 —
# 这些 nginx 都是原生能力。换成 nginx 后镜像从 ~715MB 降到 ~80MB，构建也不需要联网装 npm 包。
# `/api` 的转发由前面的 haproxy 负责（见 docker/haproxy/config/haproxy.cfg）。
FROM nginx:1.27-alpine

# SPA 回退、gzip、缓存策略
COPY nginx.conf /etc/nginx/nginx.conf

# Angular（ui/）的构建产物；`public` 这一层来自 pom 里的 copy-web-ui-dist
COPY web/public /usr/share/nginx/html

EXPOSE 8080
