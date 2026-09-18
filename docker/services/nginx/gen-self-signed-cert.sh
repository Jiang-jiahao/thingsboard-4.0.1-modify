#!/usr/bin/env bash
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
