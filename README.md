# JnksIOT 物联网平台

杰能科世物联网平台。设备接入、数据采集、规则引擎、可视化与多租户管理。

## 能力概览

- **设备接入**：MQTT、HTTP、CoAP、LwM2M、SNMP、TCP、UDP 七种传输协议
- **数据处理**：规则引擎（节点可编排）、计算字段、告警
- **可视化**：仪表盘、部件库、SCADA 符号
- **管理**：多租户、设备档案、客户、资产、实体视图
- **集成**：REST API、版本控制、通知（邮件 / SMS / 移动推送）

## 构建

需要 **JDK 17+**（推荐 21）和 **Maven 3.6.3+**。

```bash
mvn clean install -DskipTests
```

> 不要加 `-Dmaven.test.skip=true`。`apps/base` 依赖 `libs/dao` 的 test-jar，
> 完全跳过测试会让该构件不被生成，构建报
> `Could not find artifact ...:dao:jar:tests`。

打包可运行的 boot jar：

```bash
mvn -pl apps/jnks-iot-core,apps/jnks-iot-rule-engine -am package -DskipTests -Dpkg.package.phase=none
```

产物在各自 `target/jnks-iot-*-boot.jar`。

## 前端

`ui/` 不在 Maven reactor 里，要单独构建：

```bash
cd ui && npx ng build --configuration production
```

产物在 `ui/target/generated-resources/public`。

## 部署

见 [docker/DEPLOY.md](docker/DEPLOY.md) —— 整库初始化、镜像构建、compose 起栈、
nginx 网关与证书的完整步骤，以及已知的坑。

容器编排说明见 [docker/README.md](docker/README.md)。

## 模块结构

```
apps/       可运行服务：jnks-iot-core / jnks-iot-rule-engine / jnks-iot-transport（7 种协议）
            / jnks-iot-edqs / monolith / vc-executor
libs/       公共库：common（数据模型、消息、队列、缓存）、dao、rule-engine
            、transport、netty-mqtt
ui/         Angular 前端
docker/     部署编排与脚本
images/     各服务的 Docker 镜像定义
clients/    Java REST 客户端
tools/      迁移等工具
tests/      黑盒测试
```
