# 部署到一台新的 Linux 机器（Docker 方式）

这份文档描述把本仓库以**微服务 compose** 方式部署到干净 Linux 主机的完整流程，以及目前已知的限制。
（单体部署见文末。）

## 目录结构

```
docker/
├── README.md  DEPLOY.md
├── scripts/                所有 shell 脚本（可从任意目录执行，会自己切到 services/）
│
├── services/               ← 主 docker-compose.yml 的项目目录，只放主 compose 用到的
│   ├── docker-compose.yml  主编排（-f 必须排第一）
│   ├── .env                项目变量
│   ├── jnks-iot-core/            jnks-iot-core.env + conf/
│   ├── jnks-iot-rule-engine/     jnks-iot-rule-engine.env + conf/
│   ├── jnks-iot-js-executor/     jnks-iot-js-executor.env
│   ├── jnks-iot-vc-executor/     jnks-iot-vc-executor.env + conf/
│   ├── jnks-iot-transports/      各协议一个目录：coap/ http/ lwm2m/ mqtt/ snmp/ tcp/ udp/
│   │                       （各有 jnks-iot-<协议>-transport.env + conf/）
│   └── nginx/              入口网关配置与证书
│
├── log/                    ← 所有服务的运行日志（bind mount 产物，已 gitignore）
│   ├── jnks-iot-core/  jnks-iot-rule-engine/  jnks-iot-vc-executor/
│   ├── jnks-iot-transports/{coap,http,lwm2m,mqtt,snmp,tcp,udp}/
│   └── jnks-iot-monolith/  jnks-iot-edqs/
│
├── data/                   ← 运行时数据目录（bind mount 产物，已 gitignore）
│                            postgres-data/（PG 数据，PGDATA 在其下 db/）  redis-data/
│
└── 以下是**附加组件**：不在主 compose 里，各自一份 compose + env，由 .env 开关按需加载
    ├── postgres/           postgres.yml  hybrid.yml + jnks-iot-node.{postgres,hybrid}.env
    │   └── init/           建库脚本：容器首次初始化数据目录时自动执行，已有数据则跳过
    ├── redis/              redis{,-cluster,-sentinel}.yml + cache-redis*.env
    ├── kafka/              kafka.yml + kafka.env（Kafka 容器配置）kafka-client.env（TB 侧连接配置）
    ├── jnks-iot-edqs/            edqs.yml + jnks-iot-edqs.env  jnks-iot-core-edqs.env  jnks-iot-rule-engine-edqs.env
    ├── monitoring/         prometheus-grafana.yml（+ grafana/ prometheus/）
    └── jnks-iot-monolith/        单体形态的 conf/（日志见 docker/log/jnks-iot-monolith/）
```

**分界线**：`services/` 里的东西全部出现在主 `docker-compose.yml` 中；不出现的（可选数据库/缓存/队列/EDQS/监控、单体）都放在外面，各自成目录。


**`services/` 是主 compose 的项目目录**：`-f` 必须让 `docker-compose.yml` 排第一，外加的附加组件写成 `-f ../postgres/postgres.yml` 这种形式（相对路径一律以 `services/` 为基准），`./jnks-iot-core/conf`、`redis/cache-redis.env` 这些相对路径都以它为基准 —— compose 与用到的目录同层，路径才不用互相迁就。**日志是唯一例外**：它不跟服务目录同址，统一挂在 `../log/<服务>/`（见上）。

**可选组件是「compose + 它的 env」成对放在一个目录里**，按 `.env` 的开关自动加载：`DATABASE`→`postgres/`，`CACHE`→`redis/`，`JNKS_IOT_QUEUE_TYPE`→`kafka/`，`MONITORING_ENABLED`→`monitoring/`，`EDQS_ENABLED`→`jnks-iot-edqs/`。改哪块就进哪个目录；服务目录里的 `conf/` 会被挂进容器，日志则统一落到 `docker/log/<服务>/`（gitignore 的运行产物）——配置（入库、按服务就近）与运行产物（不入库、集中一处）分开。

## 部署步骤概览

没有一键脚本，按下面几节依次做（出错时按节排查）：

1. **前置条件**（§0）—— 内存 ≥10G、Docker ≥24、能拉基础镜像
2. **数据库**（§1）—— 容器首次启动自动初始化，不用手工准备
3. **取代码**（§2）
4. **构建镜像**（§3）—— 手工 `docker build`，或在别处构建后推 registry
5. **配置 `.env`**（§4）
6. **建日志目录 → 生成证书 → 起栈**（§5）
7. **验证**（§6）

下面的章节是这些步骤的展开说明，出问题时按节排查。

## 0. 前置条件

| 项 | 要求 | 说明 |
|---|---|---|
| 内存 | 给 Docker **≥ 10G**（本机跑 12G） | 完整栈是 **34 个容器**：19 个 JVM（core×2、rule-engine×2、transport×11、vc-executor×2、Kafka、ZooKeeper）+ 10 个 js-executor(Node) + web-ui×2 等。实测合计约 **9G**，其中 transport（11 个实例）4.6G、rule-engine 1.4G、core 0.8G、Kafka 0.77G。默认只给宿主一半内存，8G 会不够（见 §7）|
| CPU | ≥ 4 核 | 这些进程启动期比较吃 CPU，全部同时冷启动时网关可能短暂 502 |
| Docker | Engine ≥ 24 + compose plugin | `docker compose version` 能跑即可 |
| 网络 | 能拉 `docker.io` 或配好的加速站 | 中间件全部靠拉取：`postgres:16`、`redis:7-alpine`、`apache/kafka:3.7.0`、`zookeeper:3.8.1`、`nginx:1.27-alpine`（入口网关与前端）；构建 TB 镜像另需 `jnks-iot/openjdk17:bookworm-slim`（所有 Java 镜像的基础）。注意 bitnami 的镜像在不少加速站被白名单拦掉（`denied`），所以上游的 `bitnami/kafka`、`bitnami/redis` 都没用上 |
| 构建机 | JDK 21 + Maven ≥ 3.6.3 | 只在"在服务器上构建镜像"时需要；本机 PATH 里没有 mvn 时用 wrapper 那份 |

## 1. 数据库（容器首次启动时自动初始化）

`docker/postgres/init/01-jnks-iot-4.0.1.sql.gz` 是 **TB 4.0.1 装完之后的整库导出**：schema + 系统数据（sysadmin 账号、系统 widget、仪表盘、通知配置、租户/设备档案模板），**不含演示数据**。

postgres 容器把它挂到 `/docker-entrypoint-initdb.d/`，postgres 官方镜像**只在数据目录为空时执行**这个目录 —— 库里已经有数据就不会再跑，也不会覆盖。

这份脚本是这么来的（官方没有现成的 post-install SQL，只能自己装一遍再导）：

```bash
# 1) 装一份干净的库。别用镜像默认的 start-tb.sh —— 它写死了首次启动 install-tb.sh --loadDemo，会灌进演示设备
docker run -d --name jnks-iot-init -v jnks-iot-init-data:/data --entrypoint bash jnks-iot/jnks-iot-postgres:4.0.1 \
  -c 'start-db.sh && install-tb.sh; echo "INSTALL_EXIT=$?"; sleep 100000'
#    等日志出现 INSTALL_EXIT=0（约 1~2 分钟；期间日志里不应有 "Loading demo data"）

# 2) 导出（官方镜像是 PG12，SQL 文本向下兼容我们的 postgres:16）
docker exec jnks-iot-init pg_dump -U jnks-iot -d jnks-iot --no-owner --no-privileges \
  | gzip -9 > docker/postgres/init/01-jnks-iot-4.0.1.sql.gz

# 3) 收拾
docker rm -f jnks-iot-init && docker volume rm jnks-iot-init-data
```

要连**别的库**（从旧环境迁数据、或多个环境共用一个库）时，把连接信息写进 `docker/postgres/jnks-iot-node.postgres.env`：

```
SPRING_DRIVER_CLASS_NAME=org.postgresql.Driver
SPRING_DATASOURCE_URL=jdbc:postgresql://<host>:5432/jnks_iot
SPRING_DATASOURCE_USERNAME=<user>
SPRING_DATASOURCE_PASSWORD=<password>
DATABASE_TS_TYPE=sql
```

注意：装库这件事**只有这条 SQL 路径**。源码里的 `JnksIOTInstallApplication` 不存在，所以 `scripts/docker-install-tb.sh` 与 `docker-upgrade-tb.sh` 依旧不可用，版本升级的 schema 迁移要自己处理（§7）。

## 2. 取代码

```bash
git clone <仓库地址> && cd jnks-iot-4.0.1-modify
```

注意 `.mvn/maven.config` 必须一起带上（里面有 `-Dpkg.package.phase=none`，缺了会去跑 deb/assembly 打包）。

## 3. 构建镜像

镜像 = `apps/*/target/*-boot.jar` 装进 `images/<x>/target/` 后 `docker build`。两种做法：

### A. 在服务器上构建（推荐，免跨架构）

镜像 = `images/<x>/target/` 里备好 `Dockerfile` + 启动脚本 + boot jar，然后 `docker build`。
`images/` 下的 Maven 模块负责把这些组装进 target（`images/pom.xml` 的 `<modules>` 是完整清单）：

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export MVN=/path/to/apache-maven-3.6.3/bin/mvn

# 1) 组装镜像上下文（以 jnks-iot-core 为例；换成你要的模块即可）
"$MVN" -pl images/jnks-iot-core -am package -DskipTests -Dmaven.test.skip=true

# 2) 构建镜像
docker build -t jnks-iot/jnks-iot-core:latest images/jnks-iot-core/target
```

- 镜像名取 `docker/services/.env` 的 `DOCKER_REPO` + `JNKS_IOT_VERSION`
- 每个服务一个模块：`images/jnks-iot-core`、`images/jnks-iot-rule-engine`、`images/jnks-iot-monolith`、`images/jnks-iot-vc-executor-image`、`images/jnks-iot-transport-images/jnks-iot-<协议>-transport-image` …
- **web-ui 与 js-executor 额外需要联网拉 node 基础镜像**；web-ui 的前端产物要先单独 `ng build`（见 §7）

### B. 别处构建后推 registry

本机若是 arm64（如 Apple Silicon），推到 amd64 服务器上必须显式指定平台：

```bash
docker buildx build --platform linux/amd64 -t <registry>/jnks-iot-core:4.0.1 images/jnks-iot-core/target --push
```

`images/pom.xml` 里有个 `push-docker-amd-arm-images` profile 是做多架构推送的，可作参考。用 registry 的话把 `docker/services/.env` 的 `DOCKER_REPO` 改成 `<registry>`。

## 4. 配置 `docker/services/.env`

| 变量 | 默认 | 部署时建议 |
|---|---|---|
| `DOCKER_REPO` | `jnks-iot` | 用了 registry 就改成你的 registry 前缀 |
| `JNKS_IOT_VERSION` | `latest` | **改成具体版本号**（如 `4.0.1`），多机/回滚时分得清 |
| `JAVA_OPTS` | `-Xmx768M -Xms256M` | core / rule-engine 用。按内存算：每 JVM ≈ 堆 + 300M 开销 |
| `JAVA_OPTS_TRANSPORT` | `-Xmx256M -Xms128M` | transport 用（compose 里覆盖 `JAVA_OPTS`）。有 8 个实例，别调大 |
| `DATABASE` | `postgres` | `postgres` 或 `hybrid`（hybrid = Postgres 存实体 + Cassandra 存时序） |
| `CACHE` | `redis` | `redis` / `redis-cluster` / `redis-sentinel` |
| `JNKS_IOT_QUEUE_TYPE` | `kafka` | **微服务形态只能是 `kafka`** —— core / rule-engine / vc-executor 在代码里只有 Kafka 实现（见下方说明）。要连 Confluent Cloud 也是走 `kafka`，在 `.env` 里另加云连接参数 |
| `MONITORING_ENABLED` | `false` | 打开会额外起 Prometheus + Grafana |
| `EDQS_ENABLED` | `false` | 边缘队列同步，用不到就关 |
| `LOAD_BALANCER_NAME` | `jnks-iot-gateway` | 入口网关（nginx）的容器名 |
| `NGINX_GATEWAY_IMAGE` | `nginx:1.27-alpine` | 入口网关镜像。配置在 `docker/services/nginx/config/`，证书放 `docker/services/nginx/certs/` |

**为什么微服务只能用 kafka**：`queue.type=in-memory` 走的是「同一 JVM 内的内存队列」，只有在**单体（monolith）**形态下才成立 —— 那里 core / rule-engine / transport 都在同一个进程（monolith 依赖 `transport-base`，`JNKS_IOT_TRANSPORT_API_ENABLED` 默认开）。微服务形态下它们是独立进程，内存队列互相看不见，所以这三个应用**只提供了 Kafka 的 QueueFactory**：

| 模块 | 拥有的 QueueFactory | 可用队列 |
|---|---|---|
| jnks-iot-core / jnks-iot-rule-engine / vc-executor | 只有 `Kafka*` | **只能 kafka** |
| monolith | `Kafka*` + `InMemory*` | kafka 或 in-memory |
| jnks-iot-transport / jnks-iot-edqs | `Kafka*` + `InMemory*` | 两者都有（in-memory 仅在同进程时有意义） |

Kafka 的堆上限在 `docker/kafka/kafka.env` 的 `KAFKA_HEAP_OPTS`（默认 `-Xmx512M`）—— **它是 JVM，不设会默认吃掉 VM 内存的 1/4**，把 core/rule-engine 挤到被内核 OOM kill。

单机想给 transport 单独设堆，可以像本机验证时那样加一份覆盖文件，例如 `/tmp/docker-compose.e2e.yml` 里给各 transport 写 `JAVA_OPTS`。

## 5. 建目录、生成证书并起栈

```bash
# 下面这些脚本可以从任意目录执行（会自己切到 services/ 目录）
sudo docker/scripts/docker-create-log-folders.sh   # 需要 sudo：创建日志/数据目录并 chown 给对应 uid
bash docker/services/nginx/gen-self-signed-cert.sh         # 生成入口网关的自签证书（首次；生产可换成正式证书）
docker/scripts/docker-start-services.sh           # = docker compose -f docker-compose.yml [+ 各附加文件] up -d
```

入口网关启动时要读 `docker/services/nginx/certs/tls.pem` 和 `tls.key`：没有就先生成自签（上一条），或者把正式证书按这两个文件名放进去。

`docker-create-log-folders.sh` 用的是 `compose-utils.sh` 里的权限清单：日志目录 chown 给 **999**（= 镜像里 `jnks-iot` 用户的 uid，由基础镜像 `jnks-iot/openjdk17` 定义），Postgres 数据目录（`docker/data/postgres-data`）**999**，Redis 数据目录（`docker/data/redis-data`）**999:1000**。**如果直接手工 `docker compose up`，这些目录会被 Docker 以 root 建出来，容器内的非 root 用户写不进去**，所以这一步别省（macOS 上宿主属主不影响容器内可见的属主，见 §7）。

## 6. 验证与访问地址

```bash
docker compose ps                                     # 各服务应 running
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://<host>/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"sysadmin@jnks-iot.org","password":"sysadmin"}'   # 期望 200
```

**所有入口都经过 `nginx-gateway`**（配置在 `docker/services/nginx/config/`：`nginx.conf` + `locations.conf` + `proxy-headers.conf`）：

| 地址 | 去向 |
|---|---|
| `http://<host>/` | JnksIOT 管理界面（jnks-iot-web-ui1/2 的 nginx 静态服务） |
| `http://<host>/api/**` | jnks-iot-core1/2（REST API）。其中 `/api/v1/**` 转给 jnks-iot-http-transport1/2（设备 HTTP 接入）、`/api/images/**` 也转 core 但限流更宽 |
| `https://<host>/` | 同上走 443；证书来自 `docker/services/nginx/certs/`（默认是自签，浏览器会提示不受信任 —— 要正式证书就换成真实证书或接 certbot） |
| `<host>:1883` | MQTT 设备接入（转给 jnks-iot-mqtt-transport1/2） |
| `<host>:5683` | TCP 设备接入（转给 jnks-iot-tcp-transport1/2） |
| `<host>:5684/udp` | UDP 设备接入（转给 jnks-iot-udp-transport1/2） |

默认账号：`sysadmin@jnks-iot.org` / `sysadmin`。

**注意 UI 必须和 API 同源**：界面里的请求打到同源的 `/api/...`，而 web-ui 镜像只发静态文件、不转发 `/api`（原来 Node 版有个可选的代理开关，现在由入口网关承担）。所以入口只能走网关（或其它把 `/` 与 `/api` 收在同一端口的反向代理）—— 直接把 web-ui 容器的端口当入口用是不行的，那样页面能打开但登录和所有数据都会 404。

设备接入端口：`1883`(MQTT)、`5683`(TCP)、`5684/udp`(UDP)，以及 80/443（管理界面与设备 HTTP 都走这两个）。
**防火墙/安全组要把这些放开**（5684 是 UDP，别只开 TCP）。

## 7. 已知限制与坑

- 入口是**一个 `nginx-gateway`**（nginx 的 stream + http 两个模块），替代了原来的两个代理：`haproxy-certbot`（80/443/1883 + certbot 自动证书）与 `nginx-tcpudp`（5683/5684）。原来拆成两个，是因为 open-source haproxy 不支持 UDP 代理（`mode udp` 直接加载失败）。
- **与 haproxy 版的差异**：① 没有 certbot 自动签发 —— 证书放 `docker/services/nginx/certs/`（自签用 `nginx/gen-self-signed-cert.sh` 生成），要自动签发就自己加 certbot/acme.sh 容器；② 没有 haproxy 那个 9999 统计页（nginx 只有 `stub_status`，需要可自行加）；③ 健康检查是被动的（`max_fails` / `fail_timeout`），没有主动探测。
- 限流与黑白名单是从原 haproxy 配置**等价翻译**过来的：`/api` 100 次/10 秒 + 300 次/1 分钟、`/api/images` 1000 次/10 秒、每 IP 并发 50；私网与本机（对应原 trustlist）不限流，`5.136.0.0/13`、`217.199.254.1`（对应原 blocklist）直接 403。
- 上游用 `resolver 127.0.0.11 valid=10s` + `resolve` 动态解析：**容器重建换 IP 后 10 秒内自动跟上，不用重启网关**。代价是节点被**强杀**时，最长 10 秒内仍可能把请求发给已死的旧 IP（实测停掉一个 web-ui 副本，5 次请求里有 1 次 502）。想让窗口更小就调小 `valid` / `max_fails`；要滚动升级零中断，正确做法是先从上游摘除再停（或加主动健康检查）。
- 改 `nginx.conf` / `locations.conf` 里的路由或端口要改文件本身（官方 nginx 镜像不做环境变量替换），改完 `docker exec jnks-iot-gateway nginx -s reload` 即可。
- 目前**没有数据库升级/安装入口**（见第 1 节），版本升级需要自己处理 schema 迁移。
- 前端 `web-ui` 是独立镜像，由 **nginx 直接发静态文件**（`nginx.conf` 做 SPA 回退 + gzip）。它不转发 `/api`，API 路由由入口网关负责。前端产物来自 `ui/` 的 `ng build`（`ui/target/generated-resources/public`），`ui` 不在 Maven reactor 里，要单独构建；构建 web-ui 镜像前这个目录必须存在。镜像约 **266MB**（nginx 基础 77MB + 前端产物 156MB，其中 57MB 是 source map；不需要浏览器调试就可以把这 57MB 排除掉）。两个副本只是为了重启/升级时界面不断，跟吞吐无关。
- **Kafka 用官方 `apache/kafka:3.7.0`**（KRaft 单节点，`kafka.env` 里是标准 `KAFKA_*` 变量）。两个坑：① **`KAFKA_LOG_DIRS` 不能省** —— 官方镜像只要拿到任意 `KAFKA_*` 变量就会重写 `server.properties` 且只写 env 派生的项，缺了它 `log.dirs` 为空、启动直接 `ConfigException`；② KRaft 存储的格式化是镜像自己在每次启动时做的，不需要额外初始化步骤。`kafka.yml` 把 9092 发布到宿主，方便本机工具直连 —— 起栈前确认宿主没有别的 Kafka 占着这个端口。
- **postgres 的数据目录在宿主上**（`docker/data/postgres-data`），但 `postgres.yml` 里有两个反直觉的设置是为 macOS 准备的：① `PGDATA=/var/lib/postgresql/data/db`（挂载点下的**子目录**，不是挂载点本身）；② 容器直接 `user: "999:999"`，不走 entrypoint 的 root→gosu 降权。原因：Docker Desktop 的文件共享层里**「容器内看到的属主 = 创建该文件的 uid，chown 是空操作」，宿主上建出来的目录（含挂载点）一律显示成 root**。postgres 启动时会校验数据目录属主必须是它自己，拿挂载点当 PGDATA 就会报 `data directory has wrong ownership` 并**跳过整个初始化**（连 `/docker-entrypoint-initdb.d` 里的建库脚本都不执行，库是空的）。让 999 自己创建这个子目录就没问题。代价是宿主目录要能被 999 写（Linux 上由 `scripts/docker-create-log-folders.sh` 负责 chown）。
- **Redis 用官方 `redis:7-alpine`**（TB 侧本来就没配密码）。**`redis-cluster` / `redis-sentinel` 两个变体仍是 bitnami**，要用它们得先在能拉 bitnami 的网络里拉镜像或同样换掉。
- **内存配额必须算够（这是本栈最容易踩的坑）**：整套实测约 **9G**（34 个容器，见 §0 表），Docker VM 默认只分到宿主一半 —— 不够时的表现很有迷惑性：**内核 OOM 杀掉 JVM，但容器日志里没有任何错误、`docker inspect` 的 `OOMKilled` 也是 false**，只会看到 core/rule-engine 反复重启、`RestartCount` 一直涨，用户侧表现为**登录接口间歇 502**。判断方法：`docker stats` 看总量是否顶到 `docker info` 里的 MemTotal。**解法是给 VM 加内存（或调小各服务堆），不是停掉多实例副本** —— transport 的 `*1/*2`、core 的 `*1/*2` 都是刻意的多实例部署。堆参数现状：core/rule-engine `-Xmx768M`、transport `JAVA_OPTS_TRANSPORT=-Xmx256M`、Kafka `KAFKA_HEAP_OPTS=-Xmx512M`（Kafka 不设堆上限会默认吃掉 VM 内存的 1/4）。
- `js-executor` 是 JS 规则节点的执行器，compose 里写的是 `deploy.replicas: 10`（这套拓扑里 10 个实例实测约占 0.5G，是刻意的多实例，不是冗余）。

## 8. 单体（全在一个 JVM 里）

不用微服务那套时，`jnks-iot/jnks-iot-monolith` 镜像自带合适默认值（`zookeeper.enabled=false`、`js.evaluator=local`、`service.type=monolith`），单独一个容器即可：

```bash
docker run -d --name jnks-iot-monolith -p 8080:8080 \
  -v "$PWD/docker/jnks-iot-monolith/conf:/config" \
  -v "$PWD/docker/log/jnks-iot-monolith:/var/log/jnks-iot" \
  -e SPRING_DRIVER_CLASS_NAME=org.postgresql.Driver \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://<host>:5432/jnks_iot \
  -e SPRING_DATASOURCE_USERNAME=<user> -e SPRING_DATASOURCE_PASSWORD=<password> \
  jnks-iot/jnks-iot-monolith:latest
```

单体与微服务**是二选一的部署形态，不要同时在同一个库上跑**（两边都会去抢分区/队列）。
