#!/usr/bin/env bash
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
# 生成入口网关（docker/nginx）用的自签证书，对应原来 haproxy-certbot 镜像首次启动时
# 自动生成 default.pem 的行为。生产环境请换成挂载真实证书，或接入 certbot / acme.sh。
#
# 用法：bash docker/nginx/gen-self-signed-cert.sh
set -euo pipefail

CERT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
mkdir -p "${CERT_DIR}"

if [ -f "${CERT_DIR}/tls.pem" ] && [ -f "${CERT_DIR}/tls.key" ]; then
    echo "证书已存在：${CERT_DIR}/tls.pem（要重新生成就先删掉这两个文件）"
    exit 0
fi

# 用配置文件而非 -addext，兼容 macOS 自带的 LibreSSL
cat > "${CERT_DIR}/openssl.cnf" <<'EOF'
[req]
distinguished_name = dn
x509_extensions = v3
prompt = no
[dn]
O = thingsboard
CN = thingsboard.selfsigned.invalid
[v3]
subjectAltName = @alt
basicConstraints = critical, CA:true
keyUsage = critical, digitalSignature, keyEncipherment, keyCertSign
extendedKeyUsage = serverAuth
[alt]
DNS.1 = localhost
DNS.2 = thingsboard.selfsigned.invalid
IP.1 = 127.0.0.1
EOF

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout "${CERT_DIR}/tls.key" -out "${CERT_DIR}/tls.pem" \
    -config "${CERT_DIR}/openssl.cnf" 2>/dev/null

chmod 600 "${CERT_DIR}/tls.key"
echo "已生成自签证书："
echo "  ${CERT_DIR}/tls.pem"
echo "  ${CERT_DIR}/tls.key"
