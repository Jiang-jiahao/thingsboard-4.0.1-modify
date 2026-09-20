# Docker configuration for JnksIOT Microservices

This folder containing scripts and Docker Compose configurations to run JnksIOT in Microservices mode.

## Prerequisites

JnksIOT Microservices are running in dockerized environment.
Before starting please make sure [Docker CE](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/install/) are installed in your system.

## Installation

Before performing initial installation you can configure the type of database to be used with JnksIOT.
In order to set database type change the value of `DATABASE` variable in `.env` file to one of the following:

- `postgres` - use PostgreSQL database;
- `hybrid` - use PostgreSQL for entities database and Cassandra for timeseries database;

**NOTE**: According to the database type corresponding docker service will be deployed (see `postgres/postgres.yml`, `postgres/hybrid.yml` for details).

In order to set cache type change the value of `CACHE` variable in `.env` file to one of the following:

- `redis` - use Redis standalone cache (1 node - 1 master);
- `redis-cluster` - use Redis cluster cache (6 nodes - 3 masters, 3 slaves);
- `redis-sentinel` - use Redis sentinel cache (3 nodes - 1 master, 1 slave, 1 sentinel)

**NOTE**: According to the cache type corresponding docker service will be deployed (see `redis/redis.yml`, `redis/redis-cluster.yml`, `redis/redis-sentinel.yml` for details).

Execute the following command to create log folders for the services and chown of these folders to the docker container users. 
To be able to change user, **chown** command is used, which requires sudo permissions (script will request password for a sudo access): 

`
$ ./scripts/docker-create-log-folders.sh
`

Execute the following command to run installation:

`
$ ./scripts/docker-install-tb.sh --loadDemo
`

> ℹ️ **这条命令不用跑。** postgres 容器首次启动时会自动执行 `postgres/init/` 里的建库脚本
> （TB 4.0.1 的 schema 与系统数据），起栈即可用。要改用外部已有的库，见 [DEPLOY.md](DEPLOY.md) 第 1 节。
> 上游这套 `docker-install-tb.sh` 在本仓库用不了：镜像不认 `INSTALL_TB`，且 `JnksIOTInstallApplication` 类不存在。

Where:

- `--loadDemo` - optional argument. Whether to load additional demo data.

## Running

Execute the following command to start services:

`
$ ./scripts/docker-start-services.sh
`

After a while when all services will be successfully started you can open `http://{your-host-ip}` in you browser (for ex. `http://localhost`).
You should see JnksIOT login page.

Use the following default credentials:

- **System Administrator**: sysadmin@jnks-iot.org / sysadmin

If you installed DataBase with demo data (using `--loadDemo` flag) you can also use the following credentials:

- **Tenant Administrator**: tenant@jnks-iot.org / tenant
- **Customer User**: customer@jnks-iot.org / customer

In case of any issues you can examine service logs for errors.
For example to see JnksIOT node logs execute the following command:

`
$ docker-compose logs -f tb-core1 tb-core2 tb-rule-engine1 tb-rule-engine2 tb-mqtt-transport1 tb-mqtt-transport2
`

Or use `docker-compose ps` to see the state of all the containers.
Use `docker-compose logs --f` to inspect the logs of all running services.
See [docker-compose logs](https://docs.docker.com/compose/reference/logs/) command reference for details.

Execute the following command to stop services:

`
$ ./scripts/docker-stop-services.sh
`

Execute the following command to stop and completely remove deployed docker containers:

`
$ ./scripts/docker-remove-services.sh
`

Execute the following command to update particular or all services (pull newer docker image and rebuild container):

`
$ ./scripts/docker-update-service.sh [SERVICE...]
`

Where:

- `[SERVICE...]` - list of services to update (defined in docker-compose configurations). If not specified all services will be updated.

## Upgrading

> ⚠️ **升级流程同样是断的** —— `docker-upgrade-tb.sh` 依赖上面那套已不存在的安装/升级机制，
> 换镜像重启可以让服务跑起来，但**数据库 schema 迁移要自己处理**。详见 [DEPLOY.md](DEPLOY.md) 第 7 节。

In case when database upgrade is needed, execute the following commands:

```
$ ./scripts/docker-stop-services.sh
$ ./scripts/docker-upgrade-tb.sh --fromVersion=[FROM_VERSION]
$ ./scripts/docker-start-services.sh
```

Where:

- `FROM_VERSION` - from which version upgrade should be started. See [Upgrade Instructions](https://iot.example.com/docs/user-guide/install/upgrade-instructions) for valid `fromVersion` values.


## Monitoring

If you want to enable monitoring with Prometheus and Grafana you need to set <b>MONITORING_ENABLED</b> environment variable to <b>true</b>.
After this Prometheus and Grafana containers will be deployed. You can reach Prometheus at `http://localhost:9090` and Grafana at `http://localhost:3000` (default login is `admin` and password `foobar`).
To change Grafana password you need to update `GF_SECURITY_ADMIN_PASSWORD` environment variable at `./monitoring/grafana/config.monitoring` file.
Dashboards are loaded from `./monitoring/grafana/provisioning/dashboards` directory.

If you want to add new monitoring jobs for Prometheus update `./monitoring/prometheus/prometheus.yml` file.